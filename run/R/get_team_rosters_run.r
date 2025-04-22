rm(list = ls())

library(arrow)
source("../../src/R/get_team_rosters.r")

start_date <- 2021
end_date <- as.numeric(format(Sys.Date(), "%Y"))
filename <- paste0("../../data/team_rosters_", start_date, "_to_", end_date, ".parquet")

teams <- get_teams(2025)
colnames(teams)

roster <- mlb_rosters(team_id = 135, season = 2025, roster_type = "fullRoster")
colnames(roster)

team_rosters <- get_team_rosters(start_date, end_date)

team_rosters$team_abbreviation[team_rosters$team_abbreviation == "OAK"] <- "ATH"

write_parquet(team_rosters, filename)
