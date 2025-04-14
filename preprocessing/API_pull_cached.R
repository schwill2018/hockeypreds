library(tidyverse)
library(httr)
library(jsonlite)
library(readr)
library(dplyr)
library(parallel)
library(doParallel)

# Base file path where season folders (and the cache) will be created
base_path <- "C:/Users/schne/OneDrive/Grad School/SMU/Classes/STAT 6341/Project/M3/main/Data/"

# Grab game ids (for example, for recent seasons)
games <- GET("https://api.nhle.com/stats/rest/en/game")
games <- fromJSON(rawToChar(games$content)) %>% .[[1]]
games2 <- games[games$season >= "20152016" & games$season <= "20242025", ]
# Sort game_ids descending so the most recent season is first
game_ids <- unique(games2[, c("id", "season")]) %>% arrange(desc(season))

# Set up the cache file (will store game data keyed by game_id)
cache_file <- paste0(base_path, "cache_games.rds")
if (file.exists(cache_file)) {
  cache_data <- readRDS(cache_file)
} else {
  cache_data <- list()
}

# Initialize variables for processing
max_retries <- 3
delay_between_requests <- 0.1
all_combined_data <- list()  
current_season <- game_ids$season[1]  # using the most recent season as current
idx <- 1

# Set up parallel processing with one core
num_cores <- 1
cl <- makeCluster(num_cores)
clusterExport(cl, c("game_ids", "max_retries", "delay_between_requests", "base_path"))

# Function to process game data with improved retry logic
process_game_data <- function(i) {
  id <- game_ids$id[i]
  season <- game_ids$season[i]
  output_messages <- paste("Processing all data for game:", id, "for season:", season, "\n")
  
  combined_game_data <- list(play_by_play = NULL, boxscore = NULL, shift_data = NULL, game_id = id)
  success <- FALSE
  attempts <- 0
  
  while (!success && attempts < max_retries) {
    attempts <- attempts + 1
    output_messages <- c(output_messages, paste("Attempt", attempts, "for game ID:", id))
    
    # Play-by-play data
    play_by_play_url <- paste0("https://api-web.nhle.com/v1/gamecenter/", id, "/play-by-play")
    api_play_by_play <- httr::GET(play_by_play_url)
    
    # Boxscore data
    boxscore_url <- paste0("https://api-web.nhle.com/v1/gamecenter/", id, "/boxscore")
    api_boxscore <- httr::GET(boxscore_url)
    
    # Shift data
    shift_url <- paste0("https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=", id)
    api_shift_data <- httr::GET(shift_url)
    
    # Process and store available data
    if (api_play_by_play$status_code == 200) {
      combined_game_data$play_by_play <- jsonlite::fromJSON(rawToChar(api_play_by_play$content))
      output_messages <- c(output_messages, "Play-by-play data retrieved")
    } else {
      output_messages <- c(output_messages, "Failed to retrieve play-by-play data")
    }
    
    if (api_boxscore$status_code == 200) {
      combined_game_data$boxscore <- jsonlite::fromJSON(rawToChar(api_boxscore$content))
      output_messages <- c(output_messages, "Boxscore data retrieved")
    } else {
      output_messages <- c(output_messages, "Failed to retrieve boxscore data")
    }
    
    if (api_shift_data$status_code == 200) {
      combined_game_data$shift_data <- jsonlite::fromJSON(rawToChar(api_shift_data$content))[[1]]
      output_messages <- c(output_messages, "Shift data retrieved")
    } else {
      output_messages <- c(output_messages, "Failed to retrieve shift data")
    }
    
    # Consider boxscore data as critical for success; retry if missing
    if (!is.null(combined_game_data$boxscore)) {
      success <- TRUE
    } else {
      Sys.sleep(delay_between_requests)
    }
  }
  
  return(list(data = combined_game_data, messages = output_messages))
}

# Process each game_id with cache checking
for (i in seq_len(nrow(game_ids))) {
  game_id <- game_ids$id[i]
  season <- game_ids$season[i]
  
  # When season changes, save the current cache
  if (season != current_season) {
    write_rds(cache_data, file = cache_file)
    message(paste("Saved cache data for season", current_season))
    current_season <- season
  }
  
  # Check if this game_id is already cached
  if (as.character(game_id) %in% names(cache_data)) {
    cached_game <- cache_data[[as.character(game_id)]]
    if (!is.null(cached_game$boxscore) && !is.null(cached_game$boxscore$gameState)) {
      game_state <- tolower(cached_game$boxscore$gameState)
      # Skip processing if the gameState is neither "fut" nor "live"
      if (!(game_state %in% c("fut", "live"))) {
        next
      }
    }
  }
  
  # Process the game data if not skipped
  game_result <- parLapply(cl, list(i), process_game_data)[[1]]
  cat(paste(game_result$messages, collapse = "\n"), "\n")
  
  if (!is.null(game_result$data)) {
    # Update the cache (overwriting previous data if necessary)
    cache_data[[as.character(game_id)]] <- game_result$data
    # Optionally, add to combined data for further processing
    all_combined_data[[idx]] <- game_result$data
    idx <- idx + 1
  }
}

# Save the updated cache
write_rds(cache_data, file = cache_file)
message(paste("Saved cache data for final season", current_season))

# Stop the cluster and clean up
stopCluster(cl)
rm(game_result, games, games2, game_ids, all_combined_data, cl)
