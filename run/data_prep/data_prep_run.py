from data_prep import pivot_on_fielder, game_state_filter
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

on_second_wide_game_state_filtered_path = data_dir / "throw_home_runner_on_second_wide_game_state_filtered.parquet"
on_third_wide_game_state_filtered_path = data_dir / "throw_home_runner_on_third_wide_game_state_filtered.parquet"
on_second_wide_game_state_filtered_csv_path = data_dir / "throw_home_runner_on_second_wide_game_state_filtered.csv"
on_third_wide_game_state_filtered_csv_path = data_dir / "throw_home_runner_on_third_wide_game_state_filtered.csv"

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

# Filter by game state
on_second_wide_game_state_filted_lf = game_state_filter(on_second_wide_lf) 
on_thrid_wide_game_state_filted_lf = game_state_filter(on_third_wide_lf) 

# ==== Write Data ====
on_second_pl.write_csv(on_second_csv_path)
on_third_pl.write_csv(on_third_csv_path)

on_second_wide_lf.collect().write_parquet(on_second_wide_path)
on_third_wide_lf.collect().write_parquet(on_third_wide_path)
on_second_wide_lf.collect().write_csv(on_second_wide_csv_path)
on_third_wide_lf.collect().write_csv(on_third_wide_csv_path)

on_second_wide_game_state_filted_lf.collect().write_parquet(on_second_wide_game_state_filtered_path)
on_thrid_wide_game_state_filted_lf.collect().write_parquet(on_third_wide_game_state_filtered_path)
on_second_wide_game_state_filted_lf.collect().write_csv(on_second_wide_game_state_filtered_csv_path)
on_thrid_wide_game_state_filted_lf.collect().write_csv(on_third_wide_game_state_filtered_csv_path)
print(f"""
Created Datasets:
{on_second_csv_path}
{on_third_csv_path}
{on_second_wide_path}
{on_second_wide_csv_path}
{on_third_wide_path}
{on_third_wide_csv_path}
{on_second_wide_game_state_filtered_path}
{on_second_wide_game_state_filtered_csv_path}
{on_third_wide_game_state_filtered_path}
{on_third_wide_game_state_filtered_csv_path}
""")
