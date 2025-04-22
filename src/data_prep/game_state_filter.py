import polars as pl

def game_state_filter(on_third_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter throw_home_runner_on_* data for the game state where there is a runner on third with
    one out

    Args:
        on_third_lf (pl.LazyFrame): throw_home_runner_on_* data
    Returns:
        pl.LazyFrame (pl.LazyFrame): throw_home_runner_on_* data filtered by game state
    """
    # Filter for 1 out with runners on third and second
    on_third_filter_lf = on_third_lf.filter(
        (pl.col("pre_outs") == 1) &
        (pl.col("pre_runner_3b") == True)
    )

    return on_third_filter_lf


if __name__ == "__main__":
    import os

    print("Filering for game states with runner on third and one out...")
    
    # Directories
    data_dir = os.path.abspath("../../data/")
    on_third_path = os.path.join(data_dir, "throw_home_runner_on_third.csv")

    # Read CSV
    on_third_pl = pl.read_csv(on_third_path)

    # Convert to lazy
    on_third_lf = on_third_pl.lazy()

    # Pivot wider on fielder postion
    on_third_wide_lf = game_state_filter(on_third_lf = on_third_lf)
    print("Successfully Filtered for game states with runner on third and one out!")
