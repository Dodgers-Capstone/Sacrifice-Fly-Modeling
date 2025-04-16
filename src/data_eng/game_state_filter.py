from pybaseball import statcast
import polars as pl

def game_state_filter(start_dt: str, end_dt: str = None):
    """
    Download StatCast baseball data from a date range and filter by the game state condition
    where there is 1 out and runners are on 3rd and 2nd base.
    Args:
        start_dt: (Str) (fmt: YYYY-MM-DD) Data Start Date
        end_dt: (Str) (fmt: YYYY-MM-DD) Data End Date
    Return:
        data_filter: (pl.LazyFrame) Filtered by 1 out and runners on 3rd and 2nd base
    """
    # Download statcast baseball data
    data = statcast(start_dt=start_dt, end_dt=end_dt)
    
    # Convert to pl.LazyFrame
    data_lazy = pl.from_pandas(data).lazy()

    # Lazy filter by 1 out and runners on 3rd and 2nd base
    data_lazy_filter = data_lazy.filter(
        (pl.col("outs_when_up") == 1) &
        (pl.col("on_3b").is_not_null()) &
        (pl.col("on_2b").is_not_null()) &
        (pl.col("on_1b").is_null())
    )

    return data_lazy_filter

if __name__ == "__main__":
    # Download data since the start of the 2016 season, April 3rd, 2016.
    data_lazy_filter = game_state_filter(start_dt="2016-04-03")

    # Output the head of the pl.DataFrame
    print(data_lazy_filter.select(["outs_when_up", "on_3b", "on_2b", "on_1b"]).collect().head())
