from pybaseball import statcast_sprint_speed
from pybaseball import statcast_running_splits
from datetime import datetime
import polars as pl

def runner_sprint_data(year_start: int, year_today: int = None, min_samples: int = 50):
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

    run_split_cols = [
        "seconds_since_hit_000", "seconds_since_hit_005", "seconds_since_hit_010",
        "seconds_since_hit_015", "seconds_since_hit_020", "seconds_since_hit_025",
        "seconds_since_hit_030", "seconds_since_hit_035", "seconds_since_hit_040",
        "seconds_since_hit_045", "seconds_since_hit_050", "seconds_since_hit_055",
        "seconds_since_hit_060", "seconds_since_hit_065", "seconds_since_hit_070",
        "seconds_since_hit_075", "seconds_since_hit_080", "seconds_since_hit_085",
        "seconds_since_hit_090"
    ]

    # Download runner sprint data from a start year till the current year.
    # Stop while loop before current year for better results
    sprint_data_dict = dict()
    while year_start < year_today:
        # Download sprint splits
        sprint_split_lf = pl.LazyFrame(statcast_running_splits(year_start, min_samples))
        sprint_split_lf = sprint_split_lf.select(pl.exclude(["last_name, first_name", "name_abbrev", "team_id", "position_name", "age"]))
        sprint_split_lf = sprint_split_lf.with_columns([pl.col(col).cast(pl.Float64) for col in run_split_cols])
        sprint_split_lf = sprint_split_lf.with_columns(pl.col("player_id").cast(pl.String))
        # Download sprint speed
        sprint_speed_lf = pl.LazyFrame(statcast_sprint_speed(year_start, min_samples))
        sprint_speed_lf = sprint_speed_lf.select(pl.exclude(["last_name, first_name", "team_id", "team", "position", "age", "competitive_runs"]))
        sprint_speed_lf = sprint_speed_lf.with_columns(pl.col("player_id").cast(pl.String))
        # Merge
        sprint_merge_lf = sprint_speed_lf.join(sprint_split_lf, on=["player_id"], how="left")
        sprint_merge_lf = sprint_merge_lf.with_columns(pl.lit(year_start).alias("year"))
        sprint_data_dict[year_start] = sprint_merge_lf
        year_start += 1
    
    # Append each year to the bottom
    sprint_data_lf = pl.concat(sprint_data_dict.values())
    
    return sprint_data_lf 

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

    run_split_cols = [
        "seconds_since_hit_000", "seconds_since_hit_005", "seconds_since_hit_010",
        "seconds_since_hit_015", "seconds_since_hit_020", "seconds_since_hit_025",
        "seconds_since_hit_030", "seconds_since_hit_035", "seconds_since_hit_040",
        "seconds_since_hit_045", "seconds_since_hit_050", "seconds_since_hit_055",
        "seconds_since_hit_060", "seconds_since_hit_065", "seconds_since_hit_070",
        "seconds_since_hit_075", "seconds_since_hit_080", "seconds_since_hit_085",
        "seconds_since_hit_090"
    ]
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

    # Iterate the over run split columns names
    for col_name in run_split_cols:
        year_data = year_data.with_columns(
            pl.col(col_name).cast(pl.Float64)
        )
    
    # Append each year to the bottom
    year_data_lazy = pl.concat(year_data_dict.values())
    
    return year_data_lazy

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
    sprint_split_lf = pl.LazyFrame(statcast_running_splits(2016))
    print("\n".join(sprint_split_lf.columns))
    
    sprint_speed_lf = pl.LazyFrame(statcast_sprint_speed(2016))
    print("\n".join(sprint_speed_lf.columns))
    # Print Columns of any player that has ran at least 50 times.
    sprint_data_lf = runner_sprint_data(2016).collect().to_pandas()
    print("\n".join(sprint_data_lf.columns))
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
