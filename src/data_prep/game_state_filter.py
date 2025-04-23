import polars as pl

def game_state_filter(on_base_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter throw_home_runner_on_* data for the game state where there is a runner on third with
    one out

    Args:
        on_base_lf (pl.LazyFrame): throw_home_runner_on_* data
    Returns:
        pl.LazyFrame (pl.LazyFrame): throw_home_runner_on_* data filtered by game state
    """
    # Filter for 1 out with runners on third and second
    on_base_filter_lf = on_base_lf.filter(
        (pl.col("pre_outs") == 1)
    )

    return on_base_filter_lf
