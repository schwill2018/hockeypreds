#################################################
#################################################
#################################################
##SAVE IN TASKSCHEDULER FOLER AFTER EDITING!!!!##
#################################################
#################################################
#################################################

# Load necessary libraries
library(httr)
library(jsonlite)
library(tidyverse)
library(doParallel)

num_cores <- detectCores()
cl <- makeCluster(1)
registerDoParallel(1)

# Read the API key from the text file
f_path <- "C:/Users/schne/OneDrive/Grad School/SMU/Classes/STAT 6341/Project/M3/main"
api_key_file <- paste0(f_path, "/odds_api_key.txt")  # Adjust this to your file location
api_key <- readLines(api_key_file, warn = FALSE)
if(api_key == "") stop("API key not found in the file.")
odds_url <- paste0("https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds?regions=us&oddsFormat=american&apiKey=", api_key)

# Request odds data from the API
response <- GET(odds_url)
if(response$status_code != 200) {
  stop("Failed to retrieve odds data. Status code: ", response$status_code)
}

# Parse the JSON response
odds_json <- content(response, as = "text", encoding = "UTF-8")
odds_data <- fromJSON(odds_json, flatten = TRUE)

# Convert the JSON data into a tibble
odds_df <- as_tibble(odds_data)

# Unnest the nested lists:
# 1. Unnest 'bookmakers'
# 2. Unnest 'markets'
# 3. Unnest 'outcomes'
odds_flat <- odds_df %>%
  #disambiguate identical variables
  unnest(cols = bookmakers) %>%
  unnest(cols = markets, names_sep = "_") %>%
  unnest(cols = markets_outcomes) %>%
  mutate(pull_time = Sys.time()) %>%
  mutate(commence_time = as_datetime(commence_time)) %>%
  mutate(last_update = as_datetime(last_update)) %>%
  mutate(markets_last_update = as_datetime(markets_last_update)) %>%
  mutate(implied_prob = ifelse(price > 0, 100/(price+100), abs(price)/(abs(price)+100)))

stopCluster(cl)            
# Define the path for your appendable log file
log_file <- file.path(getwd(), "Data", "nhl_team_odds_log.csv")

# If the log file exists, read it and append the new odds; otherwise, create it
if(file.exists(log_file)) {
  existing_log <- read_csv(log_file, col_types = cols())
  updated_log <- bind_rows(existing_log, odds_flat)
  write_csv(updated_log, log_file)
} else {
  write_csv(odds_flat, log_file)
}

# Optional:
# If you need to later join these odds with your game dataset,
# ensure you standardize team names (or locale variants) so they match.
