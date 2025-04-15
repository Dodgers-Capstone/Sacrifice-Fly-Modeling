from pybaseball import statcast
from pybaseball import statcast_batter
from pybaseball import spraychart
import polars as pl


def filter_sac_fly_plays(statcast_lf: pl.LazyFrame):
    # coord_col_names = ["hc_x", "hc_y"]
    # filter_cmd_list = *[~pl.col(col).is_null() for col in coord_col_names]

    # Filter for sac fly plays
    statcast_lf = statcast_lf.filter(pl.col("events") == "sac_fly")

    # Filter for null coordinates

    return statcast_lf

# def sac_fly_coord_batter():
# 
# def sac_fly_coord_stadium():
# 
# def plot_stadium_coord(team_nickname: str, home_team_abbr: str, start_date: str, end_date: str = None):
#     """
#     Plot a spray chart of sacrifice flies hit at a given MLB team's home stadium.
# 
#     Args:
#         team_nickname (str): Nickname of the team (used for stadium overlay, e.g. 'dodgers', 'yankees')
#         home_team_abbr (str): Team's 3-letter abbreviation used in Statcast data (e.g. 'LAD', 'NYY')
#         start_dt (str): Start date (format: YYYY-MM-DD)
#         end_dt (str, optional): End date (format: YYYY-MM-DD). Defaults to '2023-10-01'.
# 
#     Returns:
#         None. Displays a spray chart.
#     """

if __name__ == "__main__":
    # download statcast data
    # statcast_lf = pl.scan_parquet("../../data/game_state_filter-2016-04-03.parquet").select("events", "hc_x", "hc_y")
    # statcast_lf = statcast("2022-01-01", "2023-01-01")
    # statcast_lf = pl.LazyFrame(statcast_lf)
    print(filter_sac_fly_plays(statcast_lf).select(["hc_x", "hc_y"]).collect().unique())#.head(10))
