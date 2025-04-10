from pybaseball import statcast_sprint_speed
from pybaseball import statcast_running_splits
from datetime import datetime
import polars as pl

def runner_sprint_speed_data(year_start: int, year_today: int = None, min_samples: int = 50):
    """
    Download runner sprint data from pybaseball for multiple years. Appends 
    years into one dataframe and selects relevant columns.

    Args:
        year_start (int): The first year to retrieve data from
        year_today (int, optional): The last year to retrieve data from, 
            exclusive. Defaults to current year if None.
        min_samples (int, optional): Minimum number of sprint opportunities 
            required. Defaults to 50.
    
    Returns:
        pl.LazyFrame: player_id, year, and sprint_speed columns for all years 
            in the specified range.
    """
    # If year not specified, use current year
    if year_today is None:
        year_today = datetime.now().year

    year_data_dict = dict()

    # Download runner sprint data from a start year till the current year.
    # Stop while loop before current year for better results
    while year_start < year_today:
        # Dictionary of pl.LazyFrame
        year_data = pl.LazyFrame(statcast_sprint_speed(year_start, min_samples))
        year_data = year_data.with_columns(pl.lit(year_start).alias("year"))
        year_data = year_data.select(
            pl.col("player_id"),
            pl.col("year"),
            pl.col("sprint_speed")
        )
        year_data_dict[year_start] = year_data
        year_start += 1
    
    year_data_lazy = pl.LazyFrame()
    
    # Append each year to the bottom
    year_data_lazy = pl.concat(year_data_dict.values())
    
    return year_data_lazy
#%%
def runner_time_split_data(year_start: int, year_today: int = None, min_samples: int = 50):
    """
    Download runner time split data, the time from home to first base at 5ft intervals,
    for multiple years. Appends years into one dataframe and selects relevant columns.
    Args:
        year_start (int): The first year to retrieve data from
        year_today (int, optional): The last year to retrieve data from, 
            exclusive. Defaults to current year if None.
        min_samples (int, optional): Minimum number of sprint opportunities 
            required. Defaults to 50.
    
    Returns:
        pl.LazyFrame: player_id, year, and split times from 0ft to 90ft.
    """
    # If year not specified, use current year
    if year_today is None:
        year_today = datetime.now().year

    year_data_dict = dict()

    # Download runner sprint data from a start year till the current year.
    # Stop while loop before current year for better results
    while year_start < year_today:
        # Dictionary of pl.LazyFrame
        year_data = pl.LazyFrame(statcast_running_splits(year_start, min_samples))
        year_data = year_data.with_columns(pl.lit(year_start).alias("year"))
        # year_data = year_data.select(
        #     pl.col("player_id"),
        #     pl.col("year"),
        #     pl.col("sprint_speed")
        # )
        year_data_dict[year_start] = year_data
        year_start += 1
    
    year_data_lazy = pl.LazyFrame()
    
    # Append each year to the bottom
    year_data_lazy = pl.concat(year_data_dict.values())
    
    return year_data_lazy

#%%

def sprint_statcast_lazy_merge(statcast_data: pl.LazyFrame, sprint_data: pl.LazyFrame):
    """
    Merge Statcast data with sprint speed data by player ID on 3rd base and year.
    
    Args:
        statcast_data (pl.LazyFrame): Statcast game data
        sprint_data (pl.LazyFrame): Sprint speed data
    
    Returns:
        pl.LazyFrame: Merged sprint speed and Statcast data for runners on third base
    """
    # Add year column
    statcast_data = (statcast_data.with_columns(
        pl.col("game_date").dt.year().alias("year"),
    )

    # Merge by playerID and year
    merged_lazy = (statcast_data
        .join(
            sprint_data,
            left_on=["on_3b", "year"],
            right_on=["player_id", "year"],
            how="left"
        )
        .with_columns(
            pl.col("sprint_speed").alias("on_3b_max_velocity_ft/s")
        )
    )

    return merged_lazy

def runner_time_to_home(statcast_data: pl.LazyFrame):
    """
    Calculate estimated time for runners to reach home plate based on sprint speed.
    
    Args:
        statcast_data (pl.LazyFrame): Statcast data with sprint_speed
    
    Returns:
        pl.LazyFrame: Adds est_time_to_home, measured in sec, to the Statcast Data. 
            90 feet divided by sprint speed (ft/sec).
    """
    # Divide distance 90 ft by the sprint speed (ft/s)
    time_to_home_lf = statcast_data.with_columns(
        (90 / pl.col("sprint_speed")).alias("est_time_to_home")
    )

    return time_to_home_lf 

if __name__ == "__main__":
    # Runner sprint Speed since 2016
    sprint_lf = runner_sprint_speed_data(2016)
    print(sprint_lf.collect().head(10))

    # Replace with game_state_filter once merged with main
    statcast_lf = pl.scan_parquet("../../data/game_state_filter-2016-04-03.parquet")
    print(statcast_lf.collect().head(10))

    # Left merged statcast with sprint speed of 3rd runner
    merged_lf = sprint_statcast_lazy_merge(statcast_lf, sprint_lf)
    print(merged_lf.collect().head(10))

    # Estimated time from 3rd to home base
    time_to_home_lf = runner_time_to_home(merged_lf)
    print(time_to_home_lf.collect().head(10))

#%%
# ===Legacy===
    # def runner_sprint_speed_data(year_start: int, year_today: int = None, min_samples: int = 50):
    #     # If year not specified, use current year
    #     if year_today is None:
    #         year_today = datetime.now().year
    # 
    #     year_data_dict = dict()
    # 
    #     # Download runner sprint data from a start year till the current year.
    #     # Stop while loop before current year for better results
    #     while year_start < year_today:
    #         # Dictionary of pl.LazyFrame
    #         year_data = pl.LazyFrame(statcast_sprint_speed(year_start, min_samples))
    #         year_data = year_data.with_columns(pl.lit(year_start).alias("year"))
    #         year_data_dict[year_start] = year_data
    #         year_start += 1
    #     
    #     year_data_lazy = pl.LazyFrame()
    #     
    #     # Append each year to the bottom
    #     year_data_lazy = pl.concat(year_data_dict.values())
    #     
    #     # Keep most recent unique speed
    #     year_data_lazy_sort = year_data_lazy.sort(
    #         by=["player_id", "year"],
    #         descending=[False, True]
    #     )
    #     year_data_lazy_unq = year_data_lazy_sort.unique(subset=["player_id"], keep="first")
    # 
    #     return year_data_lazy
# #%%
# # For now, it reads the parquet made from game_state_filter.py
# # Lazy read to pl.LazyFrame
# df_lazy = pl.scan_parquet("../../data/game_state_filter-2016-04-03.parquet")
# # data = team_fielding(2020, 2024)
# data = statcast_sprint_speed(2016, 50)
# 
# year_start = 2016
# 
# 
# #%%
# # Fetch and print column names
# print("\n".join(df_lazy.columns))
# print("\n".join(data.columns))
# 
# #%%
# # Fetch and print first 10 columns
# data.head(10)
# df_lazy.fetch(10)
# #%%
# year_start = 2016 # ==input==
# year_today = datetime.now().year # ==input-default==
# year_data_dict = dict()
# 
# # Download runner sprint data from a start year till the current year.
# # Stop while loop before current year for better results
# while year_start < year_today:
#     # Dictionary of pl.LazyFrame
#     year_data_dict[year_start] = pl.LazyFrame(
#         statcast_sprint_speed(year_start, 50)
#     ).with_columns(
#         pl.lit(year_start).alias("year")
#     )
#     year_start += 1
# 
# #%%
# year_data_lazy = pl.LazyFrame()
# 
# # Append each year to the bottom
# for key, value in year_data_dict.items():
#     print(key)
#     year_data_lazy = pl.concat([year_data_lazy, value])
# 
# #%%
# # Keep most recent unique speed
# year_data_lazy_unq = (
#     year_data_lazy
#     .sort(by=["player_id", "year"], descending=[False, True])
#     .unique(subset=["player_id"], keep="first")
#  )
# 
