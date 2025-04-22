library(purrr)
library(dplyr)
library(baseballr)

get_teams <- function(target_season = NULL) {
  # If null then current year
  if (is.null(target_season)) {
    target_season <- as.numeric(format(Sys.Date(), "%Y"))
  }

  # Fetch and select team info
  mlb_teams_info <- baseballr::mlb_teams(season = target_season, sport_ids = c(1)) |>
    dplyr::select(team_full_name, team_id, team_abbreviation, season)

  return(mlb_teams_info)
}

get_rosters <- function(start_year,
                        end_year = NULL) {
  if (is.null(end_year)) {
    end_year <- as.numeric(format(Sys.Date(), "%Y"))
  }
  years <- seq(start_year, end_year)

  rosters <- purrr::map_dfr(years, function(yr) {
    ids <- get_teams(target_season = yr)$team_id
    purrr::map_dfr(ids, function(id) {
      baseballr::mlb_rosters(team_id = id, season = yr, roster_type = "fullSeason")
    })
  })

  return(rosters)
}

get_team_rosters <- function(start_year,
                             end_year = NULL) {
  if (is.null(end_year)) {
    end_year <- as.numeric(format(Sys.Date(), "%Y"))
  }

  years_sequence <- seq(start_year, end_year)

  all_rosters <- get_rosters(start_year = start_year, end_year = end_year)

  all_teams_info <- purrr::map_dfr(years_sequence, get_teams)

  merged_data <- dplyr::left_join(all_rosters,
    all_teams_info,
    by = c("team_id", "season")
  )

  return(merged_data)
}
