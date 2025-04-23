import polars as pl

def pivot_on_fielder(on_base_pl: pl.DataFrame) -> pl.LazyFrame:
    """
    Pivots fielder data wider based on their position.

    Function takes a Polars DataFrame because pivot is an eager operation. Function drops rows with
    null fielder player ID, 'player_id', maps position IDs (7, 8, 9, 37) to position 
    labels ('LF', 'CF', 'RF', '3R'), and then pivots the DataFrame wider. Each position label is
    appended to the end of the fielder values such as positioning. The resulting dataset is wider
    and uses the play_id as the key. Contains a check if there are columns not included in pivot.

    Args:
        on_base_pl (pl.DataFrame): Takes in throw_home_runner_on_* data.
    Returns:
        pl.LazyFrame: wider throw_home_runner_on_* data.
    """
    on_base_pl_cols = set(on_base_pl.columns)

    fielder_values = ["mlb_person_id", "at_pitch_x", "at_pitch_y", "at_zone_x", "at_zone_y",
                      "at_landing_x", "at_landing_y", "at_fielded_x", "at_fielded_y",
                      "at_throw_1_x", "at_throw_1_y"]

    fielder_index = list(on_base_pl_cols - set(fielder_values))

    # Drop Null Fielder data (Note: Fielder values may still be null)
    on_base_pl = on_base_pl.filter(
        pl.col("pos_id").is_not_null() |
        pl.col("mlb_person_id").is_not_null()
    )

    # Make a column that converts position id to position names
    on_base_pl = on_base_pl.with_columns(
        pl.when(pl.col("pos_id") == 7).then(pl.lit("LF")) # Left Fielder
          .when(pl.col("pos_id") == 8).then(pl.lit("CF")) # Center Fielder
          .when(pl.col("pos_id") == 9).then(pl.lit("RF")) # Right Fielder
          .when(pl.col("pos_id") == 37).then(pl.lit("R3")) # Runner on 3rd
          .when(pl.col("pos_id") == 36).then(pl.lit("R2")) # Runner on 2nd
          .otherwise(pl.lit(None))
          .alias("pos_label")
    )

    # Pivot wider by position
    on_third_wide_pl = on_base_pl.pivot(
        values = fielder_values,
        index = fielder_index,
        on = "pos_label",
        aggregate_function = "first"
    )

    # Convert to pl.LazyFrame for potential memory efficiency and deferred execution
    on_third_wide_lf = on_third_wide_pl.lazy()

    return on_third_wide_lf
