#%% ==== Import the data ====
from pathlib import Path
import polars as pl
import os

on_third_path = "../../data/throw_home_runner_on_third_wide_sprint_game_state_filtered.parquet"

# Import on Third data
on_third_lf = pl.scan_parquet(on_third_path)

select_cols = [seconds_since_hit_090_mlb_person_id_R3, ]
on_third_lf = pl.select()

#%% ==== Data Columns ====
print("\n".join(on_third_lf.collect_schema()))

#%% ==== Encode the Variables ====
from sklearn.preprocessing import OneHotEncoder
import numpy as np
import torch

# Sample categorical data (replace with your actual data)
categorical_data_np = np.array([['high'], ['low'], ['medium'], ['high']])

encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
encoded_data_np = encoder.fit_transform(categorical_data_np)

# Convert to PyTorch tensor
encoded_data_tensor = torch.tensor(encoded_data_np, dtype=torch.float32)
print(encoded_data_tensor)
