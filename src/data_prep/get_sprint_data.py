from pybaseball import statcast_running_splits
from datetime import datetime
import polars as pl

def get_sprint_data(year_start: int,
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
                       "competitive_runs"]

    # initialize list of merged sprint speed and time split
    sprint_data_list = list()

    # Loop through year_start and year_end (inclusive)
    while year_start <= year_end:
        # Download running splits, filter data, and assign dtypes
        sprint_split_lf = pl.LazyFrame(statcast_running_splits(year_start, min_samples))
        sprint_split_lf = sprint_split_lf.drop(["position_name", "name_abbrev"])
        sprint_split_lf = sprint_split_lf.select(pl.exclude(sprint_split_ex))
        sprint_split_lf = sprint_split_lf.with_columns(type_assign_loop_split)
        sprint_split_lf = sprint_split_lf.with_columns(pl.col("player_id").cast(pl.Int64))
        sprint_split_lf = sprint_split_lf.with_columns(pl.lit(year_start).alias("curr_year"))
        sprint_split_lf = sprint_split_lf.with_columns((pl.lit(year_start) + 1).alias("prev_year"))
        sprint_data_list.append(sprint_split_lf)
        year_start += 1
    
    # Append each dataset year to the bottom
    sprint_data_lf = pl.concat(sprint_data_list)
    
    return sprint_data_lf 
