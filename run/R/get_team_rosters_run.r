rm(list = ls())

library(here)
library(arrow)
library(readr)

source_script_path <- here::here("src", "R", "get_team_rosters.r")
print(paste("Sourcing functions from:", source_script_path))
source(source_script_path)

start_date <- 2021
end_date <- as.numeric(format(Sys.Date(), "%Y"))
base_filename <- paste0("team_rosters_", start_date, "_to_", end_date)
output_dir <- here::here("data")
filename_parquet <- file.path(output_dir, paste0(base_filename, ".parquet"))
filename_csv <- file.path(output_dir, paste0(base_filename, ".csv"))

team_rosters <- get_team_rosters(start_date, end_date)

team_rosters$team_abbreviation[team_rosters$team_abbreviation == "OAK"] <- "ATH"

arrow::write_parquet(team_rosters, filename_parquet)
readr::write_csv(team_rosters, filename_csv)

print(paste("Parquet output file path:", filename_parquet))
print(paste("CSV output file path:", filename_csv))
