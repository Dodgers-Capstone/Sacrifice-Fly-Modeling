import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from pybaseball import statcast_batter
from pybaseball.plotting import plot_stadium

# --- Constants ---
HOME_PLATE_X_PLOT: float = 125.0
HOME_PLATE_Y_PLOT: float = -205.0  # Approx. home plate location in plot coordinates
SCALE_FT_TO_PLOT: float = 0.909  # Feet to plot units conversion for polar grid

# --- Function for Polar Grid ---


def add_polar_grid(
  ax: Axes,
  center_x: float,
  center_y: float,
  scale_ft_to_plot: float,
  radial_interval_ft: float = 15.0,
  max_radius_ft: float = 450.0,
  # --- New Parameter for Ray Spacing ---
  ray_arc_deg: float = 15.0,  # Angle between rays (e.g., 15 degrees)
  # ------------------------------------
  color: str = "grey",
  linestyle: str = "--",
  alpha: float = 0.6,
  linewidth: float = 0.8,
) -> None:
  """
  Adds a polar grid overlay (radii and rays) to a Matplotlib axis.
  Rays are drawn every `ray_arc_deg` around the full 360 degrees,
  based on standard mathematical angles (0=right, 90=up).
  """
  # --- Draw Radii (Circles) ---
  radii_ft = np.arange(radial_interval_ft, max_radius_ft + radial_interval_ft, radial_interval_ft)
  radii_plot = radii_ft * scale_ft_to_plot
  theta_circle = np.linspace(0, 2 * np.pi, 150)
  for r_plot in radii_plot:
    if r_plot < 1e-6:
      continue
    x_circle = center_x + r_plot * np.cos(theta_circle)
    y_circle = center_y + r_plot * np.sin(theta_circle)
    ax.plot(
      x_circle,
      y_circle,
      color=color,
      linestyle=linestyle,
      linewidth=linewidth,
      alpha=alpha,
      zorder=1,
    )

  # --- Draw Rays (Lines) covering 360 degrees ---
  # Generate angles from 0 up to 360 (exclusive) with the specified step

  angles_deg_math = np.arange(0, 360, ray_arc_deg)

  # Convert to radians for trig functions
  angles_rad_math = np.deg2rad(angles_deg_math)
  r_max_plot = max_radius_ft * scale_ft_to_plot

  for angle_rad in angles_rad_math:
    x_end = center_x + r_max_plot * np.cos(angle_rad)
    y_end = center_y + r_max_plot * np.sin(angle_rad)
    ax.plot(
      [center_x, x_end],
      [center_y, y_end],
      color=color,
      linestyle=linestyle,
      linewidth=linewidth,
      alpha=alpha,
      zorder=1,
    )


# --- Main Execution Block ---
if __name__ == "__main__":
  # --- Configuration ---
  stadium_team_abbr: str = "astros"
  statcast_team_abbr: str = "HOU"
  player_id: int = 514888
  start_dt: str = "2019-05-01"
  end_dt: str = "2019-07-01"
  # --- Ray Configuration ---
  custom_ray_arc = 15.0  # Draw a ray every 15 degrees
  # ------------------------

  # 1. Create the base stadium plot
  print(f"Plotting stadium for: {stadium_team_abbr}")
  try:
    stadium_ax: Axes = plot_stadium(stadium_team_abbr)
  except ValueError as e:
    print(f"\nError generating stadium plot: {e}")
    exit()

  # 2. Add the polar grid overlay with custom ray arc parameter
  print("Adding polar grid...")
  add_polar_grid(
    ax=stadium_ax,
    center_x=HOME_PLATE_X_PLOT,
    center_y=HOME_PLATE_Y_PLOT,
    scale_ft_to_plot=SCALE_FT_TO_PLOT,
    # --- Pass custom ray arc parameter ---
    ray_arc_deg=custom_ray_arc,
    # -----------------------------------
  )

  # 3. Fetch Statcast data
  print(f"Fetching Statcast data for player {player_id} ({start_dt} to {end_dt})...")
  try:
    data = statcast_batter(start_dt, end_dt, player_id)
  except Exception as e:
    print(f"\nError fetching Statcast data: {e}")
    data = pd.DataFrame()

  # 4. Filter data
  print(f"Filtering data for home team '{statcast_team_abbr}' and valid coordinates...")
  if not data.empty:
    sub_data = data[
      (data["home_team"] == statcast_team_abbr)
      & (data["hc_x"].notna())
      & (data["hc_y"].notna())
      & (data["type"] == "X")
    ].copy()
  else:
    sub_data = pd.DataFrame()

  print(f"Found {len(sub_data)} batted balls matching criteria.")

  # 5. Plot the hit locations if data exists
  if not sub_data.empty:
    print(f"Plotting {len(sub_data)} batted balls...")
    event_colors = {
      "single": "#008000",
      "double": "#0000FF",
      "triple": "#800080",
      "home_run": "#FF0000",
      "field_out": "#808080",
      "sac_fly": "#FFA500",
      "field_error": "#FFC0CB",
      "grounded_into_double_play": "#A0522D",
      "force_out": "#D2691E",
    }
    default_color = "#000000"
    colors = sub_data["events"].fillna("unknown").map(event_colors).fillna(default_color)

    stadium_ax.scatter(
      sub_data["hc_x"],
      -sub_data["hc_y"],  # Plot negative hc_y
      c=colors,
      s=40,
      alpha=0.7,
      zorder=5,
    )
    print("Plotting complete.")
  else:
    print("No valid batted ball data found to plot.")

  # 6. Set title and display the plot
  stadium_ax.set_title(
    f"Jose Altuve Batted Balls ({start_dt} to {end_dt}, Home Team: {statcast_team_abbr})"
  )
  try:
    plt.tight_layout()
  except UserWarning:
    pass
  plt.show()
