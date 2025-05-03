import polars as pl

def fielder_distance(on_base_lf: pl.LazyFrame,
                     home_coord_x: float,
                     home_coord_y: float) -> pl.LazyFrame:
    """
    Create new columns for the fielder who caught the ball: the fielder's coordinates, the
    fielder's distance to home plate, and the fielder's distance traveled from when the ball was
    batted to when the ball was caught by the fielder.

    Args:
        on_base_lf: (pl.LazyFrame) A widened on base dataset from pivot_on_fielder, or similar 

    Returns:
        pl.LazyFrame: The on_base dataset with additional fielder who caught the ball features
    """
    # Zone is when the ball passes the strike zone 
    # fielded is when the ball is caught by a fielder
    coordinates = ["at_zone_x", "at_zone_y", "at_fielded_x", "at_fielded_y"]

    # Get the coordinates of the fielder who caught the ball
    for coordinate in coordinates:
        on_base_lf = on_base_lf.with_columns(
            pl.when(pl.col("fielder_position") == 7).then(pl.col(f"{coordinate}_LF"))
              .when(pl.col("fielder_position") == 8).then(pl.col(f"{coordinate}_CF"))
              .when(pl.col("fielder_position") == 9).then(pl.col(f"{coordinate}_RF"))
              .alias(f"{coordinate}_fielder")
        )

    base_coords = {"home":  [home_coord_x, home_coord_y],
                  "third":  [(home_coord_x - 63.64), (home_coord_y + 63.64)],
                  "second": [(home_coord_x - 0), (home_coord_y + 127.28)], 
                  "first":  [(home_coord_x + 63.64), (home_coord_y + 63.64)]}

    # Calculate the distance of the fielder who caught the ball to each base including home
    for base, coords in base_coords.items():
        x_coord = coords[0]
        y_coord = coords[1]
        
        on_base_lf = on_base_lf.with_columns(
            ((pl.col("at_fielded_x_fielder") - (x_coord)).pow(2) + 
             (pl.col("at_fielded_y_fielder") - (y_coord)).pow(2)
            ).sqrt().alias(f"distance_catch_to_{base}")
        )
    
    fielder_positions = ["fielder", "LF", "CF", "RF"]

    # Calculate the distance traveled for each fielder from when the ball was batted to caught
    distance_traveled_cols = []
    for fielder_pos in fielder_positions:
        distance_traveled_col = f"distance_traveled_{fielder_pos}"
        # Calculate the distance fielder traveled to catch the ball from when the ball was batted
        on_base_lf = on_base_lf.with_columns(
            ((pl.col(f"at_fielded_x_{fielder_pos}") - pl.col(f"at_zone_x_{fielder_pos}")).pow(2) + 
             (pl.col(f"at_fielded_y_{fielder_pos}") - pl.col(f"at_zone_y_{fielder_pos}")).pow(2)
            ).sqrt().alias(distance_traveled_col)
        )
        distance_traveled_cols.append(distance_traveled_col)


    # Calculate the total distance traveled by fielders
    on_base_lf = on_base_lf.with_columns(
        (pl.col(distance_traveled_cols[1]) +
         pl.col(distance_traveled_cols[2]) +
         pl.col(distance_traveled_cols[3])
        ).alias("distance_traveled_all_fielders")
    )
        
    return on_base_lf
