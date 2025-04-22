import polars as pl

def pivot_on_fielder(on_third_pl: pl.DataFrame) -> pl.LazyFrame:
    """
    Pivots fielder data wider based on their position.

    Function takes a Polars DataFrame because pivot is an eager operation. Function drops rows with
    null fielder player ID, 'player_id', maps position IDs (7, 8, 9, 37) to position 
    labels ('LF', 'CF', 'RF', '3R'), and then pivots the DataFrame wider. Each position label is
    appended to the end of the fielder values such as positioning. The resulting dataset is wider
    and uses the play_id as the key. Contains a check if there are columns not included in pivot.

    Args:
        on_third_pl (pl.DataFrame): Takes in throw_home_runner_on_* data.
    Returns:
        pl.LazyFrame: wider throw_home_runner_on_* data.
    """
    fielder_values = ["player_id", "at_pitch_x", "at_pitch_y", "at_zone_x", "at_zone_y",
                      "at_landing_x", "at_landing_y", "at_fielded_x", "at_fielded_y",
                      "at_throw_1_x", "at_throw_1_y"]

    fielder_index = ["game_id", "year", "game_date", "pa_id", "play_id", "pitch_id", "is_advance",
                     "is_out", "is_stay", "fielder_credit_id", "event_type", "fielder_position",
                     "fielder_mlb_person_id", "fielder_name", "fielder_credit_type", "runner_name",
                     "fielder_team", "runner_team", "pre_runner_1b", "pre_runner_2b",
                     "pre_runner_3b", "pre_outs", "post_runner_1b", "post_runner_2b",
                     "post_runner_3b", "post_outs", "pos_code", "start_time", "start_pos_x",
                     "start_pos_y", "start_pos_z", "start_vel_x", "start_vel_y", "start_vel_z",
                     "start_speed", "end_time", "end_pos_x", "end_pos_y", "end_pos_z", "end_vel_x",
                     "end_vel_y", "end_vel_z", "end_speed", "landing_time", "landing_pos_x",
                     "landing_pos_y", "landing_pos_z", "arm_strength", "distance_covered",
                     "total_distance_covered", "throw_distance", "angle_to_ball_landing",
                     "angle_to_ball_caught", "lead_distance", "sprint_speed_runner",
                     "t_3b_to_home", "exit_speed", "vert_exit_angle", "horz_exit_angle",
                     "exit_spin_rate", "exit_spin_axis", "landing_bearing", "landing_distance",
                     "score_differential", "hang_time", "url"]

    fielder_on = "pos_id"

    # Check data and pivot columns. Columns not specified for pivot are dropped. if 
    # Unique column names
    pivot_col_list = set(fielder_values + fielder_index + [fielder_on])
    input_col_list = set(on_third_pl.columns)
    # Get the difference bettween the column names
    col_not_in_pivot = input_col_list - pivot_col_list 
    col_not_in_input = pivot_col_list - input_col_list

    #check if columns in the dataset are not in the pivot set
    if col_not_in_pivot:
        raise ValueError(
            "Input DataFrame has unexpected columns. Additional columns not specified in the"
            "pivot parameters will be dropped. Please add columns to pivot_on_fielder.py"
            f"Unexpected Columns:\n{col_not_in_pivot}"
        )

    # Check if the columns in the pivot set are not in the dataset
    if col_not_in_input:
        raise ValueError(
            "Input DataFrame is missing  columns. Cross-check with columns in pivot_on_fielder.py"
            f"Unexpected Columns:\n{col_not_in_input}"
        )

    # Drop Null Fielder data (Note: Fielder values may still be null)
    on_third_pl = on_third_pl.filter(
        pl.col("player_id").is_not_null()
    )

    # Make a column that converts position id to position names
    on_third_pl = on_third_pl.with_columns(
        pl.when(pl.col("pos_id") == 7).then(pl.lit("LF")) # Left Fielder
          .when(pl.col("pos_id") == 8).then(pl.lit("CF")) # Center Fielder
          .when(pl.col("pos_id") == 9).then(pl.lit("RF")) # Right Fielder
          .when(pl.col("pos_id") == 37).then(pl.lit("3R")) # Runner on 3rd
          .otherwise(pl.lit(None))
          .alias("pos_label")
    )

    # Pivot wider by position
    on_third_wide_pl = on_third_pl.pivot(
        values = fielder_values,
        index = fielder_index,
        on = "pos_label",
        aggregate_function = "first"
    )

    # Convert to pl.LazyFrame for potential memory efficiency and deferred execution
    on_third_wide_lf = on_third_wide_pl.lazy()

    return on_third_wide_lf

if __name__ == "__main__":
    import os

    print("Pivotting Wider...")
    
    # Directories
    data_dir = os.path.abspath("../../data/")
    on_third_path = os.path.join(data_dir, "throw_home_runner_on_third.csv")

    # Read CSV
    on_third_pl = pl.read_csv(on_third_path)

    # Pivot wider on fielder postion
    on_third_wide_lf = pivot_on_fielder(on_third_pl = on_third_pl)
    print("Successfully pivotted wider!")
