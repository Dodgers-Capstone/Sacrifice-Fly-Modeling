import polars as pl
import os
import datetime
import glob

def pivot_fielder_data(on_third_pl: pl.DataFrame) -> pl.LazyFrame:
    print(f"Initial Shape: {on_third_pl.shape}")

    fielder_values = ["player_id", "at_pitch_x", "at_pitch_y", "at_zone_x", "at_zone_y",
                      "at_landing_x", "at_landing_y", "at_fielded_x", "at_fielded_y",
                      "at_throw_1_x", "at_throw_1_y"]

    fielder_index = ["game_id", "year", "game_date", "pa_id", "play_id", "pitch_id", "is_advance",
                     "is_out", "is_stay", "fielder_credit_id", "event_type", "fielder_position",
                     "fielder_mlb_person_id", "fielder_name", "fielder_credit_type", "runner_name",
                     "fielder_team", "runner_team", "pre_runner_1b", "pre_runner_2b",
                     "pre_runner_3b", "pre_outs", "post_runner_1b", "post_runner_2b",
                     "post_runner_3b", "post_outs", "start_time", "start_pos_x", "start_pos_y",
                     "start_pos_z", "start_vel_x", "start_vel_y", "start_vel_z", "start_speed",
                     "end_time", "end_pos_x", "end_pos_y", "end_pos_z", "end_vel_x", "end_vel_y",
                     "end_vel_z", "end_speed", "landing_time", "landing_pos_x", "landing_pos_y",
                     "landing_pos_z", "arm_strength", "distance_covered", "total_distance_covered",
                     "throw_distance", "angle_to_ball_landing", "angle_to_ball_caught",
                     "lead_distance", "sprint_speed_runner", "t_3b_to_home", "exit_speed",
                     "vert_exit_angle", "horz_exit_angle", "exit_spin_rate", "exit_spin_axis",
                     "landing_bearing", "landing_distance", "score_differential", "hang_time",
                     "url"]
    
    # Drop Null Fielders data
    on_third_pl = on_third_pl.filter(
        pl.col("player_id").is_not_null()
    )

    # Make a column that converts position codes to position names
    on_third_pl = on_third_pl.with_columns(
        pl.when(pl.col("pos_id") == 7).then(pl.lit("LF"))
          .when(pl.col("pos_id") == 8).then(pl.lit("CF"))
          .when(pl.col("pos_id") == 9).then(pl.lit("RF"))
          .when(pl.col("pos_id") == 37).then(pl.lit("3F"))
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

    # Print Pivot shape
    print(f"Post-Pivot Shape: {on_third_wide_pl.shape}")

    # Convert to pl.LazyFrame
    on_third_wide_lf = on_third_wide_pl.lazy()

    return on_third_wide_lf

def player_id_on_3b(on_third_lf: pl.LazyFrame, team_rosters_lf: pl.LazyFrame) -> pl.LazyFrame:
    # Rename person_id
    team_rosters_lf = team_rosters_lf.rename({"person_id": "player_id_on_3b"})

    # Merge 1: Joining on name, year, and team
    merge_1_lf = on_third_lf.join(
        team_rosters_lf.select(["person_full_name", "player_id_on_3b", "season", "team_abbreviation"]),
        left_on=["runner_name", pl.col("year"), "runner_team"],
        right_on=["person_full_name", pl.col("season"), "team_abbreviation"],
        how="left"
    )

    # Separate successful merges and those needing a second attempt
    merged_successfully_lf = merge_1_lf.filter(
        pl.col("player_id_on_3b").is_not_null()
    )
    id_missing_lf = merge_1_lf.filter(
        pl.col("player_id_on_3b").is_null()
    )

    # Drop player_id_on_3b before the second join
    id_missing_lf_prep = id_missing_lf.drop("player_id_on_3b")

    team_rosters_miss_lf = team_rosters_lf.select(["person_full_name", "player_id_on_3b"])

    # Merge 2: Second attempt
    merge_2_lf = id_missing_lf_prep.join(
        team_rosters_miss_lf,
        left_on=["runner_name"],
        right_on=["person_full_name"],
        how="left"
    )
    
    # Append the second attempt to the first attempt
    merged_on_third_team_rosters = pl.concat([merged_successfully_lf, merge_2_lf], how="vertical")

    return merged_on_third_team_rosters

def game_state_filter(on_third_lf: pl.LazyFrame) -> pl.LazyFrame:
    # Filter for 1 out with runners on third and second
    on_third_filter_lf = on_third_lf.filter(
        (pl.col("pre_outs") == 1) &
        (pl.col("pre_runner_3b") == True) &
        (pl.col("pre_runner_2b") == True) &
        (pl.col("pre_runner_1b") == False)
    )

    return on_third_filter_lf

if __name__ == "__main__":
    # ==== Data Paths ====
    data_dir = os.path.abspath("../../data/")
    on_third_path = os.path.join(data_dir, "throw_home_runner_on_third.csv")
    team_roster = os.path.join(data_dir, "team_rosters_2021_to_2025.parquet")

    # ==== Import Data ====
    on_third_pl = pl.read_csv(on_third_path)
    team_rosters_lf = pl.scan_parquet(team_roster)

    # ==== Prep the data ====
    # Name Correction for on_third_pl
    on_third_pl = on_third_pl.with_columns(
        pl.col("runner_name").str.replace_all("Manny Pina", "Manny Piña").alias("runner_name"),
        pl.col("fielder_name").str.replace_all("Manny Pina", "Manny Piña").alias("fielder_name")
    )
    
    # Pivot wider on fielder position
    on_third_wide_lf = pivot_fielder_data(on_third_pl = on_third_pl)
    
    # Add player_id for runner on 3rd
    on_third_wide_3b_id_lf = player_id_on_3b(on_third_lf = on_third_wide_lf,
                                             team_rosters_lf = team_rosters_lf) 

    # Filter by game state
    on_third_wide_3b_id_filter_lf = game_state_filter(on_third_lf = on_third_wide_3b_id_lf) 

    # ==== Data Shape at each step ====
    # Initial Shape
    print(f"Initial shape: {on_third_pl}")

    # Shape after pivot wider
    on_third_wide_pl = on_third_wide_lf.collect()
    print(f"Shape after pivoting wider: {on_third_wide_pl}")

    # Shape after adding on_3b player_id
    on_third_wide_3b_id_pl = on_third_wide_3b_id_lf.collect()
    print(f"Schema after adding runner on 3rd player_id: {on_third_wide_3b_id_pl.shape}")

    # Shape after filtering by game state
    on_third_wide_3b_id_filter_pl = on_third_wide_3b_id_filter_lf.collect()
    print(f"Schema after filtering by game state: {on_third_wide_3b_id_filter_pl.shape}")

    # ==== Write Data ====
    # Pivoted wider and 3rd base runner player_id
    on_third_wide_3b_id_pl.write_csv("../../data/on_third.csv")
    # Pivoted wider, 3rd base runner player_id, and filtered for 1 out with runners at second and third 
    on_third_wide_3b_id_filter_pl.write_csv("../../data/on_third_game_state_filtered.csv")
