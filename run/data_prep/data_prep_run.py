import polars as pl
from pathlib import Path
import os
import glob
from data_prep import (pivot_on_fielder, game_state_filter, get_sprint_data, 
                       merge_sprint_by_position, prep_arm_strength, merge_arm_strength_by_position)
from data_eng import fielder_distance

# ==== Data Paths ====
# Script Directory Path
pos_dict = {"third": "mlb_person_id_R3", "second": "mlb_person_id_R2", "first":"mlb_person_id_R1"}

file_path = os.path.dirname(__file__)
project_path = os.path.abspath(os.path.join(file_path, "../../"))
data_path = os.path.join(project_path, "data")

# Get available running datasets
on_base_paths = []
for base in pos_dict.keys():
    on_base_path = os.path.join(data_path, f"throw_home_runner_on_{base}.parquet")   
    if os.path.exists(on_base_path):
        on_base_paths.append(on_base_path)


# Get available arm_strength datasets
arm_strength_paths = glob.glob(os.path.join(data_path, "arm_strength_*.csv"))

# Check if path lists are empty
if not on_base_paths:
    raise ValueError(f"throw_home_runner_on_<base>.parquet files not found at: {data_path}")

if not arm_strength_paths:
    raise ValueError(f"arm_strenth_<year>.csv not found at: {data_path}")

# ==== Download and Prepare Supplimental Data, Sprint and Arm Stength ====

sprint_lf = get_sprint_data(year_start = 2020)
arm_lf = prep_arm_strength(arm_strength_paths)
# print("\n".join(arm_lf.collect_schema()))

# ==== Prepare on_base data ====

on_base_lf_list = list()

for on_base_path in on_base_paths:
    # Get base of interest from file name
    file_name = os.path.basename(on_base_path)
    base = str(file_name.split("_")[-1].split(".")[0])
    print(f"Processing runner on {base} data at: {on_base_path}")

    # Read in on_base data
    on_base_pl = pl.read_parquet(on_base_path)
    # print("\n".join(on_base_pl.collect_schema()))

    # Pivot fielder features wider
    on_base_lf = pivot_on_fielder(on_base_pl)
    print(f"Widened runner on {base} by fielder features")
    # print("\n".join(on_base_pl.collect_schema()))

    # Filter for less than 2 outs
    on_base_lf = game_state_filter(on_base_lf)
    print(f"Filtered runner on {base} data for plays with less than one out")

    # Correct Manny Pina's name to match Statcast
    on_base_lf = on_base_lf.with_columns(
        pl.col("runner_name").str.replace_all("Manny Pina", "Manny Piña").alias("runner_name"),
        pl.col("fielder_name").str.replace_all("Manny Pina", "Manny Piña").alias("fielder_name")
    )

    # Get runner of interest player_id column name
    position = pos_dict[base]

    # Merge Sprint data for the runner of intereest
    on_base_lf = merge_sprint_by_position(on_base_lf = on_base_lf,
                                          sprint_data_lf = sprint_lf,
                                          position = position)

    print(f"Merged Sprint data for runner on {base} by fielder features")
    # print("\n".join(on_base_pl.collect_schema()))

    # Merge arm strengths of all out fielders and the out fielder that caught the ball
    on_base_lf = merge_arm_strength_by_position(on_base_lf = on_base_lf,
                                   arm_strength_data_lf = arm_lf,
                                   position = "mlb_person_id_LF")

    on_base_lf = merge_arm_strength_by_position(on_base_lf = on_base_lf,
                                   arm_strength_data_lf = arm_lf,
                                   position = "mlb_person_id_CF")

    on_base_lf = merge_arm_strength_by_position(on_base_lf = on_base_lf,
                                   arm_strength_data_lf = arm_lf,
                                   position = "mlb_person_id_RF")

    on_base_lf = merge_arm_strength_by_position(on_base_lf = on_base_lf,
                                   arm_strength_data_lf = arm_lf,
                                   position = "fielder_mlb_person_id")

    # Get fielder coordinates, fielder distance to home plate, fielder distance travled to catch
    on_base_lf = fielder_distance(on_base_lf = on_base_lf,
                                  home_coord_x = 0.0,
                                  home_coord_y = 0.0)

    print(f"Merged arm strength data for runner on {base} by fielder features")
    # print("\n".join(on_base_pl.collect_schema()))

    # Save engineered on_base data as parquet and csv
    mod_file_name = f"throw_home_runner_on_{base}_wide_sprint_arm"
    on_base_lf.collect().write_parquet(os.path.join(data_path, f"{mod_file_name}.parquet"))
    on_base_lf.collect().write_csv(os.path.join(data_path, f"{mod_file_name}.csv"))
    print(f"Saved data to: {data_path}")
