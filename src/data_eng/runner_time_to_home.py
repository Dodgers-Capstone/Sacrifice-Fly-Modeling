import polars as pl

#%%
# Lazy read to pl.LazyFrame
df_lazy = pl.scan_parquet("../../data/game_state_filter-2016-04-03.parquet")

#%%
# Fetch and print column names
print("\n".join(df_lazy.columns))
