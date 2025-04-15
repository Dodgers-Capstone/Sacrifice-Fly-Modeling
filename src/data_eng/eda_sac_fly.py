from pybaseball import statcast
from pybaseball import statcast_batter
from pybaseball import spraychart
from 
import polars as pl

#%%

sac_fly_coord_batter():

sac_fly_coord_stadium():

plot_stadium_coord(team_nickname: str, home_team_abbr: str, start_date: str, end_date: str = None):
    """
    Plot a spray chart of sacrifice flies hit at a given MLB team's home stadium.

    Args:
        team_nickname (str): Nickname of the team (used for stadium overlay, e.g. 'dodgers', 'yankees')
        home_team_abbr (str): Team's 3-letter abbreviation used in Statcast data (e.g. 'LAD', 'NYY')
        start_dt (str): Start date (format: YYYY-MM-DD)
        end_dt (str, optional): End date (format: YYYY-MM-DD). Defaults to '2023-10-01'.

    Returns:
        None. Displays a spray chart.
    """
