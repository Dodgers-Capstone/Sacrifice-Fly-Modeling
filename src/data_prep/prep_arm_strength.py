import os
from typing import List

import polars as pl


def prep_arm_strength(path_list: List[str]) -> pl.LazyFrame:
  """
  Prepare the arm strength data by concatenating the arm strength data and adding year columns.
  Add 2 columns, year the data was aquired and the year after the data was aquired. The year
  after is labled as previous year because from the perspective of a merge on the current year,
  it would be the previous year. Concatenate all of the data sets into one. Assumes the file name
  is arm_strength_<year>.py .

  Args:
      path_list: (str) list of absolute paths to arm stregth data.

  Returns:
      pl.LazyFrame: The concatenated arm strength data.
  """
  arm_strength_lf_list = list()
  for path in path_list:
    # Extract filename
    filename = os.path.basename(path)
    # Extract year from arm_strength_<year>.py
    year = int(filename.split("_")[-1].split(".")[0])
    # Create a year column
    select_cols = ["player_id", "total_throws", "max_arm_strength", "arm_overall"]
    arm_strength_lf = pl.scan_csv(path)
    arm_strength_lf = arm_strength_lf.select(select_cols)
    arm_strength_lf = arm_strength_lf.with_columns(pl.lit(year).alias("curr_year"))
    arm_strength_lf = arm_strength_lf.with_columns((pl.lit(year) + 1).alias("prev_year"))
    # Add to the arm_strength_lf to the list
    arm_strength_lf_list.append(arm_strength_lf)

  # Raise error if no data was added to the list
  if len(arm_strength_lf_list) < 1:
    raise ValueError("Arm strength files not found. Confirm the absolute file paths in list.")

  combined_arm_strength_lf = pl.concat(arm_strength_lf_list)

  return combined_arm_strength_lf
