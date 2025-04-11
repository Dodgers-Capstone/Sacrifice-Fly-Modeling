from pybaseball import statcast_sprint_speed
from pybaseball import statcast_running_splits
from datetime import datetime
import polars as pl

def runner_sprint_data(year_start: int, year_today: int = None, min_samples: int = 10):
    """
    Download runner sprint data from StatCast for multiple years and append 
    into one dataframe. Sprint data is a join of the sprint speed and running
    split datasets.

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

    # Download runner split and sprint speed data from the start year till the current year.
    # Stop while loop before current year for better results
    sprint_data_dict = dict()
    while year_start < year_today:
        # Download running splits, filter data, and assign dtypes
        sprint_split_lf = pl.LazyFrame(statcast_running_splits(year_start, min_samples))
        sprint_split_lf = sprint_split_lf.select(pl.exclude(["last_name, first_name", "name_abbrev", "team_id", "position_name", "age"]))
        sprint_split_lf = sprint_split_lf.with_columns([pl.col(col).cast(pl.Float64) for col in run_split_cols])
        sprint_split_lf = sprint_split_lf.with_columns(pl.col("player_id").cast(pl.Int64))
        # Download sprint speed, filter data, and assign dtypes
        sprint_speed_lf = pl.LazyFrame(statcast_sprint_speed(year_start, min_samples))
        sprint_speed_lf = sprint_speed_lf.select(pl.exclude(["last_name, first_name", "team_id", "team", "position", "age", "competitive_runs"]))
        sprint_speed_lf = sprint_speed_lf.with_columns(pl.col("player_id").cast(pl.Int64))
        # Merge on player_id, add year column, and add to dictionary
        sprint_merge_lf = sprint_speed_lf.join(sprint_split_lf, on=["player_id"], how="left")
        sprint_merge_lf = sprint_merge_lf.with_columns(pl.lit(year_start).alias("year"))
        sprint_data_dict[year_start] = sprint_merge_lf
        year_start += 1
    
    # Append each dataset year to the bottom
    sprint_data_lf = pl.concat(sprint_data_dict.values())
    
    return sprint_data_lf 

def sprint_statcast_lazy_merge(statcast_data: pl.LazyFrame, sprint_data: pl.LazyFrame, statcast_base: str):
    """
    Merge Statcast data with sprint speed and run split data
    by player ID on 3rd base and year.
    Args:
        statcast_data (pl.LazyFrame): Statcast game data
        sprint_data (pl.LazyFrame): Sprint speed data
        statcast_base (str): The column name from Statcase that contains the player ID
    Returns:
        pl.LazyFrame: Merged sprint speed and Statcast data for runners on the base of interest
    """
    # Add year column
    statcast_data = statcast_data.with_columns(pl.col("game_date").dt.year().alias("year"))
    
    # Prepend base name to sprint columns
    sprint_data = sprint_data.rename(
        {col: f"{statcast_base}_{col}" for col in sprint_data.collect_schema().names() if col not in ["player_id", "year"]}
    )

    # Merge by playerID and year
    merged_lazy = (statcast_data
        .join(
            sprint_data,
            left_on=[f"{statcast_base}", "year"],
            right_on=["player_id", "year"],
            how="left"
        )
    )

    return merged_lazy

if __name__ == "__main__":
    # Print Columns of any player that has ran at least 50 times.
    sprint_data_lf = runner_sprint_data(2016, min_samples = 10)
    print("\n===Head of Sprint Speed and Split Merged Dataset===\n")
    print(sprint_data_lf.collect().head())

    # Replace with game_state_filter once merged with main
    statcast_lf = pl.scan_parquet("../../data/game_state_filter-2016-04-03.parquet")
    print("\n===Head of Game State Filtered StatCast===\n")
    print(statcast_lf.collect().head())

    # Input Column Names
    sprint_split_lf = pl.LazyFrame(statcast_running_splits(2016))
    print("\n===Running Split Column Names===\n")
    print("\n".join(sprint_split_lf.collect_schema().names()))
    
    sprint_speed_lf = pl.LazyFrame(statcast_sprint_speed(2016))
    print("\n===Sprint Speed Column Names===\n")
    print("\n".join(sprint_speed_lf.collect_schema().names()))

    print("\n===StatCast Column Names===\n")
    print("\n".join((statcast_lf.collect_schema().names())))

    # Left merged statcast with sprint speed of 3rd runner
    on_3b_merged_lf = sprint_statcast_lazy_merge(statcast_lf, sprint_data_lf, "on_3b")
    on_2b_on_3b_merged_lf = sprint_statcast_lazy_merge(on_3b_merged_lf, sprint_data_lf, "on_2b")
    time_to_base_lf = on_2b_on_3b_merged_lf.select(["on_3b", "on_3b_hp_to_1b", "on_3b_seconds_since_hit_090", "on_2b", "on_2b_hp_to_1b", "on_2b_seconds_since_hit_090"])
    print("\n===Merged Statcast and Sprint Data Column Names===\n")
    print("\n".join((on_2b_on_3b_merged_lf.collect_schema().names())))
    print("\n===Head of Time to Next Base Dataset===\n")
    print(time_to_base_lf.collect().head(10))
