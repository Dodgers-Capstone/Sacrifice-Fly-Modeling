from pybaseball import statcast_sprint_speed
from pybaseball import statcast_running_splits
from datetime import datetime
import polars as pl

def runner_sprint_data(year_start: int,
                       year_end: int = None,
                       min_samples: int = 10):
    """
    Download runner sprint data from StatCast for multiple years and append into one dataframe. 
    Sprint data is a join of the sprint speed and running split datasets.

    Args:
        year_start (int): The first year to retrieve data from
        year_end (int, optional): The last year to retrieve data. defaults to current year.
        min_samples (int, optional): Minimum number of sprint opportunities required.
    
    Returns:
        sprint_data_lf (pl.LazyFrame): Merged sprint speed and time split by player_id and year.
    """
    # If year not specified, use current year
    if year_end is None:
        year_end = datetime.now().year

    # List of running split column names
    run_split_cols = [
        "seconds_since_hit_000", "seconds_since_hit_005", "seconds_since_hit_010",
        "seconds_since_hit_015", "seconds_since_hit_020", "seconds_since_hit_025",
        "seconds_since_hit_030", "seconds_since_hit_035", "seconds_since_hit_040",
        "seconds_since_hit_045", "seconds_since_hit_050", "seconds_since_hit_055",
        "seconds_since_hit_060", "seconds_since_hit_065", "seconds_since_hit_070",
        "seconds_since_hit_075", "seconds_since_hit_080", "seconds_since_hit_085",
        "seconds_since_hit_090"
    ]

    # list of data type assignments for running split 
    type_assign_loop_split = [pl.col(col).cast(pl.Float64) for col in run_split_cols]

    # Exclution lists
    sprint_split_ex = ["last_name, first_name", "team_id", "team", "position", "age",
                       "competitive_runs"
    ]
    sprint_speed_ex = ["last_name, first_name", "name_abbrev", "team_id", "position_name", "age"]

    # initialize list of merged sprint speed and time split
    sprint_data_list = list()

    # Loop through year_start and year_end (inclusive)
    while year_start <= year_end:
        # Download running splits, filter data, and assign dtypes
        sprint_split_lf = pl.LazyFrame(statcast_running_splits(year_start, min_samples))
        sprint_split_lf = sprint_split_lf.select(pl.exclude(sprint_split_ex))
        sprint_split_lf = sprint_split_lf.with_columns(type_assign_loop_split)
        sprint_split_lf = sprint_split_lf.with_columns(pl.col("player_id").cast(pl.Int64))

        # Download sprint speed, filter data, and assign dtypes
        sprint_speed_lf = pl.LazyFrame(statcast_sprint_speed(year_start, min_samples))
        sprint_speed_lf = sprint_speed_lf.select(pl.exclude(sprint_speed_ex))
        sprint_speed_lf = sprint_speed_lf.with_columns(pl.col("player_id").cast(pl.Int64))

        # Merge on player_id, add year column, and add to dictionary
        sprint_merge_lf = sprint_speed_lf.join(sprint_split_lf, on=["player_id"], how="full")
        sprint_merge_lf = sprint_merge_lf.with_columns((pl.lit(year_start) + 1).alias("next_year"))
        sprint_data_list.append(sprint_merge_lf)
        year_start += 1
    
    # Append each dataset year to the bottom
    sprint_data_lf = pl.concat(sprint_data_list)
    
    return sprint_data_lf 

def sprint_statcast_lazy_merge(statcast_data: pl.LazyFrame,
                               sprint_data: pl.LazyFrame,
                               statcast_position: str):
    """
    Sprint speed and time split data left merged on Statcast Data by player ID and last year's
    sprint data on the current year of statcast for columns that contain a player ID. For Example,
    the variable on_3b is the player ID for the runner on 3rd base. The sprint data column names
    are prefixed with the position name.

    Args:
        statcast_data (pl.LazyFrame): Statcast game data
        sprint_data (pl.LazyFrame): Sprint speed data
        statcast_position (str): The column name from Statcase that contains the player ID
    Returns:
        merged_lazy (pl.LazyFrame): Sprint data left merged on Statcast data
    """
    # Add year column
    statcast_data = statcast_data.with_columns(pl.col("game_date").dt.year().alias("year"))
    
    # Dictionary for renaming sprint data to player postion
    rename_dict = dict()
    col_names_ex_prefix = ["player_id", "next_year"]
    col_names_to_prefix = [col for col in sprint_data.collect_schema().names() if col not in col_names_ex_prefix]
    prefix_rename_dict = {col: f"{statcast_position}_{col}" for col in col_names_to_prefix}

    # Prepend base name to sprint columns
    sprint_data = sprint_data.rename(prefix_rename_dict)

    # Merge by playerID and year
    merged_lazy = (
        statcast_data.join(sprint_data, left_on=[f"{statcast_position}", "year"], right_on=["player_id", "next_year"], how="left")
    )

    return merged_lazy

if __name__ == "__main__":
    from game_state_filter import game_state_filter

    # Replace with game_state_filter once merged with main
    statcast_lf = game_state_filter("2021-04-01")

    # Sprint Data for previous year at least 10 times.
    sprint_data_lf = runner_sprint_data(2020, min_samples = 10)

    # Left merged statcast with sprint speed of 3rd runner
    on_3b_merged_lf = sprint_statcast_lazy_merge(statcast_lf, sprint_data_lf, "on_3b")
    on_2b_on_3b_merged_lf = sprint_statcast_lazy_merge(on_3b_merged_lf, sprint_data_lf, "on_2b")

    # Show the home-to-1st time and 90ft split for the players on 3rd and 2rd
    time_to_base_lf = on_2b_on_3b_merged_lf.select(["on_3b", "on_3b_hp_to_1b", "on_3b_seconds_since_hit_090", "on_2b", "on_2b_hp_to_1b", "on_2b_seconds_since_hit_090"])
    print(time_to_base_lf.collect().head(10))
