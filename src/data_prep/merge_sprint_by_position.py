import polars as pl

def merge_sprint_by_position(on_base_lf: pl.LazyFrame,
                             sprint_data_lf: pl.LazyFrame,
                             position: str):
    """
    Sprint speed and time split data left merged on Statcast Data by player ID and last year's
    sprint data on the current year of statcast for columns that contain a player ID.

    Args:
        on_base_lf (pl.LazyFrame): Statcast game data
        sprint_data_lf (pl.LazyFrame): Sprint speed data
        position (str): The column name contains the MLB player ID
    Returns:
        merged_lazy (pl.LazyFrame): Sprint data left merged on Statcast data
    """
    # Dictionary for renaming sprint data to player postion
    rename_dict = dict()
    col_names_ex_suffix = ["player_id", "curr_year", "prev_year"]
    col_names_to_suffix = [col for col in sprint_data_lf.collect_schema().names() if col not in col_names_ex_suffix]
    suffix_rename_dict = {col: f"{col}_{position}" for col in col_names_to_suffix}

    # Prepend base name to sprint columns
    sprint_data_lf = sprint_data_lf.rename(suffix_rename_dict)

    # Merge by playerID and year using current year's sprint data
    merged_lf = on_base_lf.join(
        sprint_data_lf,
        left_on=[position, "year"],
        right_on=["player_id", "curr_year"],
        how="left"
    )

    # Merge by playerID and year using last year's sprint data
    merged_lf = merged_lf.join(
        sprint_data_lf,
        left_on=[position, "year"],
        right_on=["player_id", "prev_year"],
        how="left",
        suffix="_prev_year"
    )

    # Suffixed columns
    base_suffixed_cols = list(suffix_rename_dict.values())

    # Coalesce
    coalesce_exprs = [
        pl.coalesce(pl.col(col_name), pl.col(f"{col_name}_prev_year")).alias(col_name)
        for col_name in base_suffixed_cols
        if f"{col_name}_prev_year" in merged_lf.collect_schema() # Ensure the prev_year column exists after the join
    ]

    # Apply coalesce and select final columns
    if coalesce_exprs:
       merged_lf = merged_lf.with_columns(coalesce_exprs)
    
    cols_to_drop = [f"{col_name}_prev_year" for col_name in base_suffixed_cols if f"{col_name}_prev_year" in merged_lf.collect_schema()]
    cols_to_drop.extend(['player_id_prev_year', 'prev_year', 'curr_year'])

    # Check actual column names before dropping
    final_cols_to_drop = [c for c in cols_to_drop if c in merged_lf.collect_schema()]

    merged_lf = merged_lf.drop(final_cols_to_drop)

    return merged_lf
