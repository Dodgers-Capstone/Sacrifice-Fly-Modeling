import polars as pl


def merge_arm_strength_by_position(
  on_base_lf: pl.LazyFrame, arm_strength_data_lf: pl.LazyFrame, position: str
) -> pl.LazyFrame:
  """
  Merge Statcast aggregate arm strength data to the on_base dataset by position MLB IDs that
  contain the position's player_id and the year of the game. If the current year aggregate arm
  strength data is missing, suppliment it with the previous year's arm strength aggregate data.
  Overall arm strength is the average of top 10% of throws.

  Args:
      on_base_lf: (pl.LazyFrame) The throw_home_runner_on_<base> dataset.
      arm_strength_data_lf: (pl.LazyFrame) arm_strength dataset from prep_arm_strength.py
      position: (str) position column name containing the MLB player ID
  Returns:
      pl.LazyFrame: Position  overall and max arm strength merged with throw_home_runner_on_<base>
  """
  # Dictionary for renaming arm_strength_data to player position
  col_names_ex_suffix = ["player_id", "curr_year", "prev_year"]
  col_names_to_suffix = [
    col for col in arm_strength_data_lf.collect_schema().names() if col not in col_names_ex_suffix
  ]
  suffix_rename_dict = {col: f"{col}_{position}" for col in col_names_to_suffix}

  arm_strength_data_lf = arm_strength_data_lf.rename(suffix_rename_dict)

  # Merge by playerID and year using current year's sprint data
  merged_lf = on_base_lf.join(
    arm_strength_data_lf,
    left_on=[position, "year"],
    right_on=["player_id", "curr_year"],
    how="left",
  )

  # Merge by playerID and year using last year's sprint data
  merged_lf = merged_lf.join(
    arm_strength_data_lf,
    left_on=[position, "year"],
    right_on=["player_id", "prev_year"],
    how="left",
    suffix="_prev_year",
  )

  # Suffixed columns
  base_suffixed_cols = list(suffix_rename_dict.values())

  # Coalesce polars command to suppliment the current year with the previous year if the current
  # year is missing
  coalesce_exprs = [
    pl.coalesce(pl.col(col_name), pl.col(f"{col_name}_prev_year")).alias(col_name)
    for col_name in base_suffixed_cols
    if f"{col_name}_prev_year"
    in merged_lf.collect_schema()  # Ensure the prev_year column exists after the join
  ]

  # Apply coalesce
  if coalesce_exprs:
    merged_lf = merged_lf.with_columns(coalesce_exprs)

  # Drop column names with the suffix, "_prev_year"
  cols_to_drop = [
    f"{col_name}_prev_year"
    for col_name in base_suffixed_cols
    if f"{col_name}_prev_year" in merged_lf.collect_schema()
  ]
  # Also drop the duplicate columns
  cols_to_drop.extend(["player_id_prev_year", "prev_year", "curr_year"])

  # Check actual column names before dropping
  final_cols_to_drop = [c for c in cols_to_drop if c in merged_lf.collect_schema()]

  merged_lf = merged_lf.drop(final_cols_to_drop)

  return merged_lf
