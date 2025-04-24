from data_prep import pivot_on_fielder, game_state_filter, get_sprint_data, merge_sprint_by_position
import polars as pl
from pathlib import Path
import os

# ==== Data Paths ====
# Script Directory Path
base_path = Path(__file__).resolve().parent

# Data Path
data_dir = (base_path / "../../data").resolve()

# Data Read Paths
on_second_path = data_dir / "throw_home_runner_on_second.parquet"
on_third_path = data_dir / "throw_home_runner_on_third.parquet"

# Data Write Paths
on_second_csv_path = data_dir / "throw_home_runner_on_second.csv"
on_third_csv_path = data_dir / "throw_home_runner_on_third.csv"

on_second_wide_path = data_dir / "throw_home_runner_on_second_wide.parquet"
on_third_wide_path = data_dir / "throw_home_runner_on_third_wide.parquet"
on_second_wide_csv_path = data_dir / "throw_home_runner_on_second_wide.csv"
on_third_wide_csv_path = data_dir / "throw_home_runner_on_third_wide.csv"

on_second_wide_sprint_path = data_dir / "throw_home_runner_on_second_wide_sprint.parquet"
on_third_wide_sprint_path = data_dir / "throw_home_runner_on_third_wide_sprint.parquet"
on_second_wide_sprint_csv_path = data_dir / "throw_home_runner_on_second_wide_sprint.csv"
on_third_wide_sprint_csv_path = data_dir / "throw_home_runner_on_third_wide_sprint.csv"

on_second_wide_sprint_game_state_filtered_path = data_dir / "throw_home_runner_on_second_wide_sprint_game_state_filtered.parquet"
on_third_wide_sprint_game_state_filtered_path = data_dir / "throw_home_runner_on_third_wide_sprint_game_state_filtered.parquet"
on_second_wide_sprint_game_state_filtered_csv_path = data_dir / "throw_home_runner_on_second_wide_sprint_game_state_filtered.csv"
on_third_wide_sprint_game_state_filtered_csv_path = data_dir / "throw_home_runner_on_third_wide_sprint_game_state_filtered.csv"

sprint_data_path = data_dir / "sprint_data.parquet"
sprint_data_csv_path = data_dir / "sprint_data.csv"

# ==== Import/Get Data ====
on_second_pl = pl.read_parquet(on_second_path)
on_third_pl = pl.read_parquet(on_third_path)

# ==== Prep the data ====
# Name Correction for on_third_pl
on_third_pl = on_third_pl.with_columns(
    pl.col("runner_name").str.replace_all("Manny Pina", "Manny Piña").alias("runner_name"),
    pl.col("fielder_name").str.replace_all("Manny Pina", "Manny Piña").alias("fielder_name")
)

# Name Correction for on_second_pl
on_second_pl = on_second_pl.with_columns(
    pl.col("runner_name").str.replace_all("Manny Pina", "Manny Piña").alias("runner_name"),
    pl.col("fielder_name").str.replace_all("Manny Pina", "Manny Piña").alias("fielder_name")
)


# Pivot wider on fielder position
on_second_wide_lf = pivot_on_fielder(on_second_pl)
on_third_wide_lf = pivot_on_fielder(on_third_pl)

# Add sprint data
sprint_lf = get_sprint_data(year_start = 2020)
on_second_wide_sprint_lf = merge_sprint_by_position(on_base_lf = on_second_wide_lf,
                                                    sprint_data_lf = sprint_lf,
                                                    position = "mlb_person_id_R2")

on_third_wide_sprint_lf = merge_sprint_by_position(on_base_lf = on_third_wide_lf,
                                                   sprint_data_lf = sprint_lf,
                                                   position = "mlb_person_id_R3")

# Filter by game state
on_second_wide_sprint_game_state_filted_lf = game_state_filter(on_second_wide_sprint_lf) 
on_thrid_wide_sprint_game_state_filted_lf = game_state_filter(on_third_wide_sprint_lf) 

# ==== Write Data ====
on_second_pl.write_csv(on_second_csv_path)
on_third_pl.write_csv(on_third_csv_path)

on_second_wide_lf.collect().write_parquet(on_second_wide_path)
on_second_wide_lf.collect().write_csv(on_second_wide_csv_path)
on_third_wide_lf.collect().write_csv(on_third_wide_csv_path)
on_third_wide_lf.collect().write_parquet(on_third_wide_path)

on_second_wide_sprint_lf.collect().write_parquet(on_second_wide_sprint_path)
on_second_wide_sprint_lf.collect().write_csv(on_second_wide_sprint_csv_path)
on_third_wide_sprint_lf.collect().write_csv(on_third_wide_sprint_csv_path)
on_third_wide_sprint_lf.collect().write_parquet(on_third_wide_sprint_path)

on_second_wide_sprint_game_state_filted_lf.collect().write_parquet(on_second_wide_sprint_game_state_filtered_path)
on_second_wide_sprint_game_state_filted_lf.collect().write_csv(on_second_wide_sprint_game_state_filtered_csv_path)
on_thrid_wide_sprint_game_state_filted_lf.collect().write_csv(on_third_wide_sprint_game_state_filtered_csv_path)
on_thrid_wide_sprint_game_state_filted_lf.collect().write_parquet(on_third_wide_sprint_game_state_filtered_path)

sprint_lf.collect().write_parquet(sprint_data_path)
sprint_lf.collect().write_csv(sprint_data_csv_path)

print(f"""
Created Datasets!

Original Dataset:
{on_second_csv_path}
{on_third_csv_path}

Pivoted Wider Dataset:
{on_second_wide_path}
{on_second_wide_csv_path}
{on_third_wide_path}
{on_third_wide_csv_path}

Pivoted Wider with Sprint:
{on_second_wide_sprint_path}
{on_second_wide_sprint_csv_path}
{on_third_wide_sprint_path}
{on_third_wide_sprint_csv_path}

Pivoted Wider with Sprint and Filtered for less than 2 Outs:
{on_second_wide_sprint_game_state_filtered_path}
{on_second_wide_sprint_game_state_filtered_csv_path}
{on_third_wide_sprint_game_state_filtered_path}
{on_third_wide_sprint_game_state_filtered_csv_path}

Sprint Dataset:
{sprint_data_path}
{sprint_data_csv_path}
""")
