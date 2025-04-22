import polars as pl

def add_player_id_3R(on_third_lf: pl.LazyFrame, team_rosters_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merges third base runner player ID from team rosters to throw_home_runner_on_* data.

    Join the `player_id` from `team_rosters_lf` onto throw_home_runner_on_* data.
    Merge by runner name, year, and team abbreviation. For failed merges, merge by runner's name.
    Append the 2 merge sets into one dataset.

    Args:
        on_third_lf (pl.LazyFrame): throw_home_runner_on_* data
        team_rosters_lf (pl.LazyFrame): team roster information

    Returns:
        pl.LazyFrame: throw_home_runner_on_* data with the player IDs of the third base runner, 
        'player_id_on_3b' column where matches were found in `team_rosters_lf`. Rows that couldn't
        be matched in either step will have null values in 'player_id_on_3b'.
    """
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

if __name__ == "__main__":
    from download_team_rosters import download_team_rosters 
    import os

    print("Adding Third Base Runner Player ID")
    
    # ==== Data Paths ====
    data_dir = os.path.abspath("../../data/")
    run_dir = os.path.abspath("../../run/data_prep")
    get_team_rosters_path = os.path.join(run_dir, "get_team_rosters_run.r")
    on_third_path = os.path.join(data_dir, "throw_home_runner_on_third.csv")
    team_roster = os.path.join(data_dir, "team_rosters_2021_to_2025.parquet")
    
    # ==== Download Team Roster data ====
    download_team_rosters(get_team_rosters_path)

    # ==== Import Data ====
    on_third_pl = pl.read_csv(on_third_path)
    team_rosters_lf = pl.scan_parquet(team_roster)

    # ==== Prep the data ====
    # convert to lazy
    on_third_lf = on_third_pl.lazy()

    # Name Correction for on_third_pl
    on_third_pl = on_third_pl.with_columns(
        pl.col("runner_name").str.replace_all("Manny Pina", "Manny Piña").alias("runner_name"),
        pl.col("fielder_name").str.replace_all("Manny Pina", "Manny Piña").alias("fielder_name")
    )
    
    # Add player_id for runner on 3rd
    on_third_wide_3b_id_lf = add_player_id_3R(on_third_lf = on_third_lf,
                                             team_rosters_lf = team_rosters_lf) 
    print("Successfully added player IDs for third base runners!")
