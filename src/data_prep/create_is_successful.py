import polars as pl
import re

def create_is_successful(on_base: pl.LazyFrame) -> pl.LazyFrame:
    """
    Create the target feature `is_successful`. `is_successful` True when the play `event_type`
    containes sac_fly.
    
    Args:
        on_base: (pl.LazyFrame) throw_home_runner_on_* data
    Returns:
      pl.LazyFrame (pl.LazyFrame): throw_home_runner_on_* data with `is_successful` target feature.
    """
    on_base = on_base.with_columns(
        pl.col("event_type").str.starts_with("sac_fly").alias("is_successful")
    )

    return on_base
    
