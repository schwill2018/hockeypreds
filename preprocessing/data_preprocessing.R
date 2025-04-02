# ------PREPROCESSING---------------------------------------------------------------------------------------------------------------------------------------------
## ------LOAD & DEFINE DATA---------------------------------------------------------------------------------------------------------------------------------------------
library(plotly)
library(reactable)
library(gt)
library(purrr)
library(future)
library(future.callr)
library(sftime)
library(lubridate)
library(lutz)
library(tmaptools)
library(geosphere)
library(furrr)
library(slider)
library(zoo)
library(pracma)
library(foreach)
library(TTR)
library(tmaptools)
library(tidygeocoder)
library(tidymodels)
library(purrr)
library(dplyr)
library(stringr)
library(jsonlite)

# Specify the path to the directory containing your RDS files
rds_files_path <- "C:/Users/schne/OneDrive/Grad School/SMU/Classes/STAT 6341/Project/M3/main/Data/"
rds_files <- list.files(rds_files_path, pattern = "*.rds", full.names = TRUE)

# Full path length for reference (to pull only yearly data)
path_len <- nchar("C:/Users/schne/OneDrive/Grad School/SMU/Classes/STAT 6341/Project/M3/main/Data/all_combined_data_20242025.rds")

# Extract the season years from the file names
season_years <- as.numeric(substr(rds_files, start = path_len - 11, stop = path_len - 4))

# Filter for the target season (e.g., 20242025) season_years >= 20092010 & 
rds_files <- rds_files[season_years >= 20092010 & season_years <= 20242025]
rds_files <- rds_files[!is.na(rds_files)]  # Drop any NA file paths

# Empty lists to store data for each type
all_plays       <- list()
all_boxscore    <- list()
all_shifts      <- list()
unplayed_games  <- list()  # For games that haven't been played yet
all_columns_plays   <- character()
all_columns_boxscore <- character()
all_columns_shifts   <- character()
k <- 1  # Index for play-by-play data
b <- 1  # Index for boxscore data
s <- 1  # Index for shift data
u <- 1  # Index for unplayed games

# Loop over each RDS file for the specified season(s)
for (rds_file in rds_files) {
  message(paste("Processing RDS file:", rds_file))
  season_data <- readRDS(rds_file)
  
  # Loop through each game in the season_data list
  for (i in seq_along(season_data)) {
    
    # Determine whether game data exists
    has_pbp   <- !is.null(season_data[[i]]$play_by_play$plays) && length(season_data[[i]]$play_by_play$plays) > 0
    has_box   <- !is.null(season_data[[i]]$boxscore$playerByGameStats) && length(season_data[[i]]$boxscore$playerByGameStats) > 0
    has_shift <- !is.null(season_data[[i]]$shift_data) && length(season_data[[i]]$shift_data) > 0
    
    ### Process played games ###
    
    # Process play-by-play if available
    if (has_pbp) {
      plays_df <- season_data[[i]]$play_by_play$plays
      
      # Add game metadata
      plays_df$game_id         <- season_data[[i]]$game_id
      plays_df$game_date       <- season_data[[i]]$play_by_play$gameDate
      plays_df$otInUse         <- season_data[[i]]$play_by_play$otInUse
      plays_df$shootoutInUse   <- season_data[[i]]$play_by_play$shootoutInUse
      plays_df$venueLocation   <- season_data[[i]]$play_by_play$venueLocation$default
      plays_df$venueUTCOffset  <- season_data[[i]]$play_by_play$venueUTCOffset
      plays_df$easternUTCOffset<- season_data[[i]]$play_by_play$easternUTCOffset
      plays_df$startTimeUTC    <- season_data[[i]]$play_by_play$startTimeUTC
      plays_df$gameType        <- season_data[[i]]$play_by_play$gameType
      
      # Extract team info from play-by-play
      home_team_abbrev <- season_data[[i]]$play_by_play$homeTeam$abbrev
      away_team_abbrev <- season_data[[i]]$play_by_play$awayTeam$abbrev
      home_team_id     <- season_data[[i]]$play_by_play$homeTeam$id
      away_team_id     <- season_data[[i]]$play_by_play$awayTeam$id
      home_score       <- season_data[[i]]$play_by_play$homeTeam$score
      away_score       <- season_data[[i]]$play_by_play$awayTeam$score
      
      plays_df$home_id         <- home_team_id
      plays_df$away_id         <- away_team_id
      plays_df$home_score      <- home_score
      plays_df$away_score      <- away_score
      
      plays_df$home_team_name <- ifelse(
        !is.null(season_data[[i]]$play_by_play$homeTeam$name),
        season_data[[i]]$play_by_play$homeTeam$name$default,
        season_data[[i]]$play_by_play$homeTeam$commonName$default)
      plays_df$away_team_name <- ifelse(
        !is.null(season_data[[i]]$play_by_play$awayTeam$name),
        season_data[[i]]$play_by_play$awayTeam$name$default,
        season_data[[i]]$play_by_play$awayTeam$commonName$default)
      
      plays_df$home_team_locale<- season_data[[i]]$play_by_play$homeTeam$placeName$default
      plays_df$away_team_locale<- season_data[[i]]$play_by_play$awayTeam$placeName$default
      
      # Determine the winning team
      plays_df$winning_team_id <- ifelse(away_score > home_score, away_team_id,
                                         ifelse(home_score > away_score, home_team_id, NA))
      plays_df$winning_team    <- ifelse(away_score > home_score, away_team_abbrev,
                                         ifelse(home_score > away_score, home_team_abbrev, NA))
      
      # Add eventOwnerTeam and league info
      plays_df <- plays_df %>%
        mutate(eventOwnerTeam = case_when(
          details$eventOwnerTeamId == home_team_id ~ home_team_abbrev,
          details$eventOwnerTeamId == away_team_id ~ away_team_abbrev,
          TRUE ~ NA_character_
        )) %>%
        mutate(league = ifelse(
          !is.null(season_data[[i]]$play_by_play$homeTeam$logo),
          str_extract(season_data[[i]]$play_by_play$homeTeam$logo, "(?<=logos/)[^/]+"),
          NA_character_
        ))
      
      # Flatten any nested fields
      plays_df <- jsonlite::flatten(plays_df)
      
      # Track unique columns and append to the list
      all_columns_plays <- unique(c(all_columns_plays, colnames(plays_df)))
      all_plays[[k]] <- plays_df
      k <- k + 1
    }
    
    # Process boxscore if available
    if (has_box) {
      # Extract and combine home and away player stats
      home_forwards <- season_data[[i]]$boxscore$playerByGameStats$homeTeam$forwards
      home_defense  <- season_data[[i]]$boxscore$playerByGameStats$homeTeam$defense
      home_goalies  <- season_data[[i]]$boxscore$playerByGameStats$homeTeam$goalies
      away_forwards <- season_data[[i]]$boxscore$playerByGameStats$awayTeam$forwards
      away_defense  <- season_data[[i]]$boxscore$playerByGameStats$awayTeam$defense
      away_goalies  <- season_data[[i]]$boxscore$playerByGameStats$awayTeam$goalies
      
      forwards_df <- bind_rows(home_forwards, away_forwards)
      forwards_df$main_position <- "forward"
      defense_df <- bind_rows(home_defense, away_defense)
      defense_df$main_position <- "defense"
      goalies_df <- bind_rows(home_goalies, away_goalies)
      goalies_df$main_position <- "goalie"
      
      boxscore_df <- bind_rows(forwards_df, defense_df, goalies_df)
      
      # Add game metadata
      boxscore_df$game_id        <- season_data[[i]]$game_id
      boxscore_df$game_date      <- season_data[[i]]$boxscore$gameDate
      boxscore_df$otInUse        <- season_data[[i]]$play_by_play$otInUse
      boxscore_df$shootoutInUse  <- season_data[[i]]$play_by_play$shootoutInUse
      boxscore_df$venueLocation  <- season_data[[i]]$boxscore$venueLocation$default
      boxscore_df$venueUTCOffset <- season_data[[i]]$boxscore$venueUTCOffset
      boxscore_df$easternUTCOffset <- season_data[[i]]$boxscore$easternUTCOffset
      boxscore_df$startTimeUTC   <- season_data[[i]]$boxscore$startTimeUTC
      boxscore_df$gameType       <- season_data[[i]]$play_by_play$gameType
      
      # Extract team info from boxscore
      home_team_abbrev <- season_data[[i]]$boxscore$homeTeam$abbrev
      away_team_abbrev <- season_data[[i]]$boxscore$awayTeam$abbrev
      home_team_id     <- season_data[[i]]$boxscore$homeTeam$id
      away_team_id     <- season_data[[i]]$boxscore$awayTeam$id
      home_score       <- season_data[[i]]$boxscore$homeTeam$score
      away_score       <- season_data[[i]]$boxscore$awayTeam$score
      
      boxscore_df$home_id        <- home_team_id
      boxscore_df$away_id        <- away_team_id
      boxscore_df$home_score     <- home_score
      boxscore_df$away_score     <- away_score
      
      boxscore_df$home_team_name <- ifelse(
        !is.null(season_data[[i]]$boxscore$homeTeam$name),
        season_data[[i]]$boxscore$homeTeam$name$default,
        season_data[[i]]$boxscore$homeTeam$commonName$default)
      boxscore_df$away_team_name <- ifelse(
        !is.null(season_data[[i]]$boxscore$awayTeam$name),
        season_data[[i]]$boxscore$awayTeam$name$default,
        season_data[[i]]$boxscore$awayTeam$commonName$default)
      
      boxscore_df$home_team_locale<- season_data[[i]]$boxscore$homeTeam$placeName$default
      boxscore_df$away_team_locale<- season_data[[i]]$boxscore$awayTeam$placeName$default
      
      # Determine the winning team
      boxscore_df$winning_team_id <- ifelse(away_score > home_score, away_team_id,
                                            ifelse(home_score > away_score, home_team_id, NA))
      boxscore_df$winning_team <- ifelse(away_score > home_score, away_team_abbrev,
                                         ifelse(home_score > away_score, home_team_abbrev, NA))
      
      # Extract league information
      boxscore_df <- boxscore_df %>%
        mutate(league = ifelse(
          !is.null(season_data[[i]]$boxscore$homeTeam$logo),
          str_extract(season_data[[i]]$boxscore$homeTeam$logo, "(?<=logos/)[^/]+"),
          NA_character_
        ))
      
      boxscore_df <- jsonlite::flatten(boxscore_df)
      
      all_columns_boxscore <- unique(c(all_columns_boxscore, colnames(boxscore_df)))
      all_boxscore[[b]] <- boxscore_df
      b <- b + 1
    }
    
    # Process shift data if available
    if (has_shift) {
      shift_df <- season_data[[i]]$shift_data
      shift_df$game_id   <- season_data[[i]]$game_id
      shift_df$game_date <- season_data[[i]]$boxscore$gameDate
      shift_df$gameType  <- season_data[[i]]$play_by_play$gameType
      shift_df$winning_team_id <- ifelse(away_score > home_score, away_team_id,
                                         ifelse(home_score > away_score, home_team_id, NA))
      shift_df$winning_team    <- ifelse(away_score > home_score, away_team_abbrev,
                                         ifelse(home_score > away_score, home_team_abbrev, NA))
      
      shift_df <- shift_df %>%
        mutate(league = ifelse(
          !is.null(season_data[[i]]$play_by_play$homeTeam$logo),
          str_extract(season_data[[i]]$play_by_play$homeTeam$logo, "(?<=logos/)[^/]+"),
          NA_character_
        ))
      
      all_columns_shifts <- unique(c(all_columns_shifts, colnames(shift_df)))
      all_shifts[[s]] <- shift_df
      s <- s + 1
    }
    
    ### Capture unplayed games ###
    # If both play-by-play and boxscore are missing, consider the game unplayed.
    game_state <- season_data[[i]]$boxscore$gameState
    if (game_state %in% c("LIVE", "FUT") || (!has_pbp) || (!has_box)) {
      game_id <- season_data[[i]]$game_id
      # Attempt to get game_date from available fields; else leave as NA
      game_date <- NA
      if (!is.null(season_data[[i]]$play_by_play) && !is.null(season_data[[i]]$play_by_play$gameDate)) {
        game_date <- season_data[[i]]$play_by_play$gameDate
      } else if (!is.null(season_data[[i]]$boxscore) && !is.null(season_data[[i]]$boxscore$gameDate)) {
        game_date <- season_data[[i]]$boxscore$gameDate
      }
      
      # Prefer rosters if available; otherwise, try to extract team info from play-by-play or boxscore.
      if (!is.null(season_data[[i]]$rosters)) {
        home_team <- season_data[[i]]$rosters$homeTeam
        away_team <- season_data[[i]]$rosters$awayTeam
      } else if (!is.null(season_data[[i]]$play_by_play)) {
        home_team <- season_data[[i]]$play_by_play$homeTeam
        away_team <- season_data[[i]]$play_by_play$awayTeam
      } else if (!is.null(season_data[[i]]$boxscore)) {
        home_team <- season_data[[i]]$boxscore$homeTeam
        away_team <- season_data[[i]]$boxscore$awayTeam
      } else {
        home_team <- list(id = NA, abbrev = NA, name = list(default = NA), roster = NA)
        away_team <- list(id = NA, abbrev = NA, name = list(default = NA), roster = NA)
      }
      
      # Extract venue info if available
      venue <- NA
      if (!is.null(game_id) & !is.na(game_date) & difftime(game_date, Sys.Date(), unit = "days") >= 0) {
        venue <- season_data[[i]]$play_by_play$venueLocation$default
      } else if (!is.null(season_data[[i]]$boxscore) && !is.null(season_data[[i]]$boxscore$venueLocation)) {
        venue <- season_data[[i]]$boxscore$venueLocation$default
      }
      
      
      if (!is.null(game_id) && !is.na(game_date) && game_state %in% c("LIVE", "FUT") &&
          difftime(season_data[[i]]$play_by_play$startTimeUTC, Sys.Date(), unit = "hours") >= -10) {
        print("Future Game")
        # Build the unplayed game record with essential metadata and roster details
        unplayed_game <- list(
          game_id          = game_id,
          game_date        = game_date,
          home_id     = home_team$id,
          away_id     = away_team$id,
          home_team_abbrev = if(!is.null(home_team$abbrev)) home_team$abbrev else NA,
          away_team_abbrev = if(!is.null(away_team$abbrev)) away_team$abbrev else NA,
          home_team_name   = if(!is.null(home_team$commonName)) home_team$commonName$default else NA,
          away_team_name   = if(!is.null(away_team$commonName)) away_team$commonName$default else NA,
          home_team_locale = season_data[[i]]$play_by_play$homeTeam$placeName$default,
          away_team_locale = season_data[[i]]$play_by_play$awayTeam$placeName$default,
          venueLocation    = venue,
          gameType         = season_data[[i]]$play_by_play$gameType,
          venueUTCOffset   = season_data[[i]]$play_by_play$venueUTCOffset,
          easternUTCOffset = season_data[[i]]$play_by_play$easternUTCOffset,
          startTimeUTC     = season_data[[i]]$play_by_play$startTimeUTC,
          league = ifelse(
            !is.null(season_data[[i]]$play_by_play$homeTeam$logo),
            str_extract(season_data[[i]]$play_by_play$homeTeam$logo, "(?<=logos/)[^/]+"),
            NA_character_)
        )
        unplayed_games[[u]] <- unplayed_game
        u <- u + 1
      }
    }
  }
  gc()
}

rm(away_defense, away_forwards, away_goalies, away_team, home_defense, home_forwards, home_goalies, home_team)
rm(defense_df, forwards_df, goalies_df, plays_df, shift_df, boxscore_df)
rm(season_data,unplayed_game)
gc()

# Ensure every play-by-play dataframe has the full set of columns
for (j in seq_along(all_plays)) {
  missing_cols <- setdiff(all_columns_plays, colnames(all_plays[[j]]))
  if (length(missing_cols) > 0) {
    all_plays[[j]][missing_cols] <- NA
  }
  all_plays[[j]] <- all_plays[[j]][, all_columns_plays]
}

# Ensure every boxscore dataframe has the full set of columns
for (j in seq_along(all_boxscore)) {
  missing_cols <- setdiff(all_columns_boxscore, colnames(all_boxscore[[j]]))
  if (length(missing_cols) > 0) {
    all_boxscore[[j]][missing_cols] <- NA
  }
  all_boxscore[[j]] <- all_boxscore[[j]][, all_columns_boxscore]
}

# Ensure every shift dataframe has the full set of columns
for (j in seq_along(all_shifts)) {
  missing_cols <- setdiff(all_columns_shifts, colnames(all_shifts[[j]]))
  if (length(missing_cols) > 0) {
    all_shifts[[j]][missing_cols] <- NA
  }
  all_shifts[[j]] <- all_shifts[[j]][, all_columns_shifts]
}

# Combine the lists of dataframes into single dataframes for played games
all_plays_df <- bind_rows(all_plays)
all_boxscore <- all_boxscore[sapply(all_boxscore, nrow) > 0]
all_boxscore_df <- bind_rows(all_boxscore)
all_shift_df <- bind_rows(all_shifts)
unplayed_games <- bind_rows(unplayed_games)

rm(all_plays, all_boxscore, all_shifts)

all_boxscore_df <- all_boxscore_df[all_boxscore_df$league == 'nhl',]
all_boxscore_df <- all_boxscore_df[all_boxscore_df$gameType %in% c(2, 3), ]
all_plays_df <- all_plays_df[all_plays_df$league == 'nhl',]
all_plays_df <- all_plays_df[all_plays_df$gameType %in% c(2, 3), ]
all_shift_df <- all_shift_df[all_shift_df$league == 'nhl',]
all_shift_df <- all_shift_df[all_shift_df$gameType %in% c(2, 3), ]
unplayed_games <- unplayed_games[unplayed_games$league == 'nhl',]
unplayed_games <- unplayed_games[unplayed_games$gameType %in% c(2, 3), ]

# Save the processed played-game data
saveRDS(all_plays_df, file = paste0(rds_files_path, "combined_2009_2024_plays_org.rds"))
saveRDS(all_boxscore_df, file = paste0(rds_files_path, "combined_2009_2024_boxscore_org.rds"))
saveRDS(all_shift_df, file = paste0(rds_files_path, "combined_2009_2024_shifts_org.rds"))

# Save the unplayed games (roster/schedule data) separately
saveRDS(unplayed_games, file = paste0(rds_files_path, "combined_2009_2024_unplayed_games_org.rds"))
gc()

# ---Progress Point----
# Load required libraries
library(dplyr)
library(lubridate)
library(parallel)
library(doParallel)

num_cores <- as.numeric(detectCores())-3
cl <- makeCluster(num_cores)
registerDoParallel(cl)
rds_files_path <- paste0(getwd(),"/Data/")

# Load the data files
all_shift_df <- readRDS(paste0(rds_files_path, "combined_2009_2024_shifts_org.rds"))
all_plays_df <- readRDS(paste0(rds_files_path, "combined_2009_2024_plays_org.rds"))
all_boxscore_df <- readRDS(paste0(rds_files_path, "combined_2009_2024_boxscore_org.rds"))
unplayed_games <- readRDS(paste0(rds_files_path, "combined_2009_2024_unplayed_games_org.rds"))

stopCluster(cl)
gc()

## ---BOXSCORE DATA------------------------------------------------------------------------------------------------------------------------------------------------
num_cores <- detectCores()-2
cl <- makeCluster(num_cores)
registerDoParallel(cl)
# Select relevant columns from all_boxscore_df and remove unnecessary columns
all_boxscore_df <- all_boxscore_df %>%
  mutate(game_date = as.Date(game_date))

##Add player respective team ID to the dataframe
# Select only the relevant columns from all_shift_df to avoid duplicates
team_reference <- all_shift_df %>%
  select(gameId, playerId, teamId) %>%
  distinct(gameId, playerId, .keep_all = TRUE)  # Ensure there are no duplicate rows

# Perform the join on gameId and playerId
all_boxscore_df <- all_boxscore_df %>%
  left_join(team_reference, by = c("game_id" = "gameId", "playerId" = "playerId"))

#Filter out preseason and all-star games
all_boxscore_df <- all_boxscore_df %>%
  filter(!is.na(teamId))

all_boxscore_df <- all_boxscore_df %>%
  select(-c(name.fi, name.sv, name.de, name.es, name.fr, name.cs, name.sk)) %>%
  # Replace missing TOI (time on ice) values with "00:00" and convert TOI to seconds
  replace_na(list(toi = "00:00")) %>%
  mutate(
    toi_mins = as.numeric(gsub("\\:.*", "", toi)) * 60,
    toi_secs = as.numeric(gsub(".*\\:", "", toi)),
    toi_real = toi_mins + toi_secs,
    season = as.integer(substr(game_id, 1, 4))  # Extract season from game_id
  ) %>%
  # Remove intermediate columns used for TOI calculations
  select(-toi_mins, -toi_secs)

all_boxscore_df <- all_boxscore_df %>%
  group_by(playerId, game_id) %>%
  arrange(game_date) %>%
  mutate(toi_real_lag1 = lag(toi_real, 1)) %>%
  ungroup()

# Create a distinct dataset with key player and position information from all_boxscore_df
distinct_boxscore <- all_boxscore_df %>%
  distinct(game_id, playerId, main_position, position)

rm(team_reference)
stopCluster(cl)
gc()


## -----PLAY-BY-PLAY DATA----------------------------------------------------------------------------------------------------------------------------------------------
cl <- makeCluster(num_cores)
registerDoParallel(cl)
# Add event outcome column based on whether the event-owning team won or lost the game
all_plays_df <- all_plays_df %>%
  mutate(eventGameOutcomeTeam = ifelse(details.eventOwnerTeamId == winning_team_id, "Winner", "Loser"))

# Merge play-by-play data with player position information from the boxscore data
all_plays_df <- all_plays_df %>%
  left_join(distinct_boxscore,
            by = c("game_id" = "game_id", "details.shootingPlayerId" = "playerId")) %>%
  rename(
    shootingPlayer_main_position = main_position,
    shootingPlayer_position = position
  ) %>%
  # Extract season information from game_id and create a goal indicator
  mutate(
    season = as.integer(substr(game_id, 1, 4)),
    isGoal = ifelse(typeDescKey == 'goal', 1, 0)
  )
stopCluster(cl)
gc()

## --SHIFT DATA-------------------------------------------------------------------------------------------------------------------------------------------------
cl <- makeCluster(num_cores)
registerDoParallel(cl)
# all_shift_df <- all_shift_df_org
rm(all_shift_df_org)
rm(all_plays_df_org)
rm(all_boxscore_df_org)

all_shift_df <- all_shift_df %>%
  mutate(game_date = as.Date(game_date))

# Replace missing values in shift duration with "00:00" and convert duration to seconds
all_shift_df <- all_shift_df %>%
  replace_na(list(duration = "00:00")) %>%
  mutate(
    shift_mins = as.numeric(gsub("\\:.*", "", duration)) * 60,
    shift_secs = as.numeric(gsub(".*\\:", "", duration)),
    shift_real = shift_mins + shift_secs,
    season = as.integer(substr(gameId, 1, 4))  # Extract season from gameId
  ) %>%
  # Remove intermediate columns used for shift duration calculations
  select(-shift_mins, -shift_secs)

# Step 2: Calculate per-game aggregated statistics (median, average, cumulative TOI per game)
game_level_stats <- all_shift_df %>%
  group_by(playerId, season, gameId, game_date) %>%
  summarise(
    med_shift_toi_per_game = median(shift_real, na.rm = TRUE),
    avg_shift_toi_per_game = mean(shift_real, na.rm = TRUE),
    cumulative_toi = sum(shift_real, na.rm = TRUE),
    n_shifts = n(),   # Count the number of shifts)
    .groups = "drop"
  ) %>%
  ungroup()

# Merge the lagged calculations back into the original dataset (if needed)
all_shift_df_test <- all_shift_df %>%
  left_join(game_level_stats, by = c("playerId", "season", "gameId", "game_date"))

stopCluster(cl)

all_shift_df <- all_shift_df_test %>%
  arrange(playerId,season)
# rm(sanity_check_shift)
rm(all_shift_df_test)
gc()

### ----Join Shift Data to Boxscore-----------------------------------------------------------------------------------------------------------------------------------------------
cl <- makeCluster(num_cores)
registerDoParallel(cl)

# Step 1: Select Only Unique Columns from the Engineered Shift Data for Joining
# This ensures no overlap with `all_boxscore_df`
unique_shift_stats <- setdiff(names(game_level_stats), names(all_boxscore_df))
all_shift_aggregated <- game_level_stats %>%
  select(playerId, gameId, all_of(unique_shift_stats))

# Step 2: Join the Aggregated Shift Data with the Boxscore Data
# Ensures no expansion of rows by maintaining the structure of `all_boxscore_df`
all_boxscore_df_final <- all_boxscore_df %>%
  left_join(all_shift_aggregated, by = c("playerId" = "playerId", "game_id" = "gameId"))

# Stop the cluster
stopCluster(cl)

all_boxscore_df <- all_boxscore_df_final

rm(all_boxscore_df_final)
rm(game_level_stats)
rm(all_shift_aggregated)
gc()

### -----Join Position Information with Shift Data----------------------------------------------------------------------------------------------------------------------------------------------
# Add position information from distinct_boxscore to all_shift_df
all_shift_df <- all_shift_df %>%
  left_join(distinct_boxscore, by = c("gameId" = "game_id", "playerId"))

rm(distinct_boxscore)
# Check for missing positions after the join
missing_positions <- all_shift_df %>%
  filter(is.na(position))

# # Output the number of missing positions if any
# print(missing_positions)
# paste0("Number of rows with missing positions: ", nrow(missing_positions))

# Filter out rows with missing position information in all_shift_df
all_shift_df <- all_shift_df %>%
  filter(!is.na(position))

# Remove rows where gameId is 2021020513 (potentially invalid or unwanted data)
all_shift_df <- all_shift_df %>%
  filter(gameId != 2021020513)
gc()


### ------------------- RECENT PLAYER STATS (ROSTER) -------------------
# Get the most recent stats (i.e., most recent roster) from your historical player data.
# Here we group by team and player and take the row with the max game_date.
recent_season <- all_boxscore_df %>% 
  mutate(season = as.integer(season)) %>% 
  pull(season) %>% 
  max(na.rm = TRUE)

recent_player_stats <- all_boxscore_df %>%
  group_by(teamId, playerId) %>%
  filter(season == recent_season)  %>%
  filter(game_date == max(game_date, na.rm = TRUE)) %>%
  select(-c(game_id, game_date, startTimeUTC, venueLocation, venueUTCOffset,
            easternUTCOffset, home_team_name, away_team_name, home_team_locale,
            away_team_locale, home_id, away_id, league)) %>%
  ungroup()

### ------------------- EXPAND UNPLAYED GAMES TO PLAYER-LEVEL -------------------
# (Optional) Ensure key ID columns are character type if needed for joining
unplayed_games <- unplayed_games %>%
  # mutate(home_team_id = as.character(home_team_id),
  #        away_team_id = as.character(away_team_id)) %>%
  mutate(game_date = as.Date(game_date))  %>%
  filter(league == "nhl")

# 1. For unplayed games, join the most recent stats for the home team.
unplayed_games_home <- unplayed_games %>%
  left_join(recent_player_stats, by = c("home_id"="teamId")) %>%
  rename(teamId = home_id) %>%
  mutate(home_id = teamId)

# 2. Similarly, join for the away team.
unplayed_games_away <- unplayed_games %>%
  left_join(recent_player_stats, by = c("away_id" = "teamId"))  %>%
  rename(teamId = away_id) %>%
  mutate(away_id = teamId)

# 3. Combine the two so each unplayed game is expanded to player level.
unplayed_games_expanded <- bind_rows(unplayed_games_home, unplayed_games_away) %>%
  mutate(game_status = "unplayed")  # Mark these rows as unplayed

### ------------------ MARK PLAYED GAMES ------------------
played_games <- all_boxscore_df %>%
  mutate(game_status = "played")  # Tag played games

### ------------------ COMBINE DATA FOR ROLLING CALCULATIONS ------------------
# Bind played and unplayed games together. This allows the rolling functions to "see" all past data.
#check to make sure no columns left out then remove extra columns from unplayed_games
setdiff(names(played_games),names(unplayed_games_expanded))
coldrops <- setdiff(names(unplayed_games_expanded),names(played_games))
unplayed_games_expanded <- unplayed_games_expanded %>% 
  select(-all_of(coldrops))
combined_games <- bind_rows(played_games, unplayed_games_expanded) %>%
  arrange(season, teamId, game_date)

all_boxscore_df <- combined_games
rm(combined_games, played_games, unplayed_games, unplayed_games_home, 
   unplayed_games_away, unplayed_games_expanded, recent_player_stats, recent_season)

# ----- STEP 3: Compute Lagged and Rolling Metrics on the Joined Data -----
X <- 5  # Define the window size for lag/rolling calculations

# Order by player and game date so that lag() works correctly
all_boxscore_df <- all_boxscore_df %>%
  arrange(playerId, season, game_date) %>%
  group_by(playerId, season) %>%
  mutate(
    # Simple lagged values for the most recent previous game
    lagged_med_shift_toi = lag(med_shift_toi_per_game, default = 0),
    lagged_avg_shift_toi = lag(avg_shift_toi_per_game, default = 0),
    lagged_n_shift = lag(n_shifts, default = 0),
    lagged_game_toi      = lag(cumulative_toi, default = 0),
    
    # Rolling metrics: for the first X-1 games, use cumulative statistics,
    # then use the rolling window of the last X games.
    lagged_med_shift_toi_last_X_games = sapply(1:n(), function(i) {
      if (i == 1) {
        0
      } else if (i < X) {
        median(med_shift_toi_per_game[1:(i-1)], na.rm = TRUE)
      } else {
        median(med_shift_toi_per_game[(i - X):(i-1)], na.rm = TRUE)
      }
    }),
    lagged_avg_shift_toi_last_X_games = sapply(1:n(), function(i) {
      if (i == 1) {
        0
      } else if (i < X) {
        mean(avg_shift_toi_per_game[1:(i-1)], na.rm = TRUE)
      } else {
        mean(avg_shift_toi_per_game[(i - X):(i-1)], na.rm = TRUE)
      }
    }),
    lagged_cum_shift_toi_last_X_games = sapply(1:n(), function(i) {
      if (i == 1) {
        0
      } else if (i < X) {
        sum(cumulative_toi[1:(i-1)], na.rm = TRUE)
      } else {
        sum(cumulative_toi[(i - X):(i-1)], na.rm = TRUE)
      }
    }),
    lagged_avg_n_shift_last_X_games = sapply(1:n(), function(i) {
      if (i == 1) {
        0
      } else if (i < X) {
        mean(n_shifts[1:(i-1)], na.rm = TRUE)
      } else {
        mean(n_shifts[(i - X):(i-1)], na.rm = TRUE)
      }
    })
  ) %>%
  ungroup()


## ----Time Since Last Game-----------------------------------------------------------------------------------------------------------------------------------------------
### Calculate Time Since Last Game for each team and each player
cl <- makeCluster(num_cores)
registerDoParallel(cl)
# Extract unique game start times from boxscore data
box_times <- all_boxscore_df %>%
  select(game_id, season, startTimeUTC) %>%
  distinct()

#### TEAM ####
# Calculate time since the last game for each team
btwn_games_time <- all_shift_df %>%
  left_join(box_times, by = c("game_id", "season")) %>%
  select(teamId, season, game_id,startTimeUTC) %>%
  distinct() %>%
  arrange(teamId, season, startTimeUTC) %>%
  group_by(teamId, season) %>%
  mutate(
    team_hrs_since_last_game = as.numeric(interval(lag(startTimeUTC), startTimeUTC) / dminutes(1))/60,
    team_hrs_since_last_game = ifelse(is.na(team_hrs_since_last_game), 1440,team_hrs_since_last_game) # Assign 1440 hours for the first game of the season
  ) %>%
  ungroup() %>%
  select(-startTimeUTC)


#### PLAYERS ####

player_btwn_games_time <- all_shift_df %>%
  left_join(box_times, by = c("game_id", "season")) %>%
  select(playerId, season, game_id, startTimeUTC) %>%
  distinct() %>%
  arrange(playerId, season, startTimeUTC) %>%
  group_by(playerId, season) %>%
  mutate(
    player_hrs_since_last_game = as.numeric(interval(lag(startTimeUTC), startTimeUTC) / dminutes(1)) / 60,
    player_hrs_since_last_game = ifelse(is.na(player_hrs_since_last_game), 1440, player_hrs_since_last_game) # 1440 hrs for the first game of the season
  ) %>%
  ungroup() %>%
  select(-startTimeUTC)


stopCluster(cl)
rm(box_times)
gc()


###----------------Separate Time Since Last Game by Home and Away Teams-----------------------------------------------------------------------------------------------------------------------------------
# Create home team time since last game data
home_time <- btwn_games_time %>%
  rename(home_id = teamId, home_hrs_since_last_game = team_hrs_since_last_game)

# Create away team time since last game data
away_time <- btwn_games_time %>%
  rename(away_id = teamId, away_hrs_since_last_game = team_hrs_since_last_game)

# Join home and away time data with all_boxscore_df
all_boxscore_df <- all_boxscore_df %>%
  left_join(home_time, by = c("home_id" = "home_id", "season", "game_id")) %>%
  left_join(away_time, by = c("away_id" = "away_id", "season", "game_id")) %>%
  left_join(player_btwn_games_time, by = c("playerId", "season", "game_id")) %>%
  mutate(
    home_hrs_since_last_game = ifelse(is.na(home_hrs_since_last_game), 1440, home_hrs_since_last_game),
    away_hrs_since_last_game = ifelse(is.na(away_hrs_since_last_game), 1440, away_hrs_since_last_game),
    player_hrs_since_last_game = ifelse(is.na(player_hrs_since_last_game), 1440, player_hrs_since_last_game)
  )

rm(btwn_games_time)
rm(player_btwn_games_time)


### --Save-------------------------------------------------------------------------------------------------------------------------------------------------
saveRDS(all_plays_df, file = paste0(rds_files_path, "combined_2009_2024_plays_v2.rds"))
saveRDS(all_boxscore_df, file = paste0(rds_files_path, "combined_2009_2024_boxscore_v2.rds"))
saveRDS(all_shift_df, file = paste0(rds_files_path, "combined_2009_2024_shifts_v2.rds"))
rm(all_plays_df, all_shift_df, home_time, away_time)
gc()


## ----Team Metrics-----------------------------------------------------------------------------------------------------------------------------------------------
#### Load
library(dplyr)
library(zoo)
library(slider)

cl <- makeCluster(num_cores)
registerDoParallel(cl)
rds_files_path <- getwd()
# Load the data files
# all_shift_df <- readRDS("/users/willschneider/Hockey Project/Data/combined_2009_2024_shifts_v2.rds")
# all_plays_df <- readRDS("/users/willschneider/Hockey Project/Data/combined_2009_2024_plays_v2.rds")
all_boxscore_df <- readRDS(paste0(rds_files_path,"/Data/combined_2009_2024_boxscore_v2.rds"))

stopCluster(cl)


### --Convert Save Percentage Columns-------------------------------------------------------------------------------------------------------------------------------------------------
cl <- makeCluster(num_cores)
registerDoParallel(cl)
# Define a function to convert "saves/shots" format to a decimal
convert_to_save_percentage <- function(column) {
  # Split the values into two parts: saves and shots
  saves_shots <- str_split_fixed(column, "/", 2)
  
  # Convert each part to numeric
  saves <- as.numeric(saves_shots[, 1])
  shots <- as.numeric(saves_shots[, 2])
  
  # Calculate the save percentage, handling division by zero
  ifelse(!is.na(shots) & shots != 0, saves / shots, 0)
}

# Apply the function to each column that needs conversion
all_boxscore_df <- all_boxscore_df %>%
  mutate(
    evenStrengthShotsAgainst = convert_to_save_percentage(evenStrengthShotsAgainst),
    powerPlayShotsAgainst = convert_to_save_percentage(powerPlayShotsAgainst),
    shorthandedShotsAgainst = convert_to_save_percentage(shorthandedShotsAgainst),
    saveShotsAgainst = convert_to_save_percentage(saveShotsAgainst)
  )

stopCluster(cl)


### -----Replace Missing Values----------------------------------------------------------------------------------------------------------------------------------------------
# Replace NAs with 0 for relevant columns in all_boxscore_df
all_boxscore_df <- all_boxscore_df %>%
  mutate(across(c(goals, assists, points, plusMinus, pim, hits, powerPlayGoals, sog,
                  faceoffWinningPctg, blockedShots, shifts, giveaways, takeaways,
                  evenStrengthShotsAgainst, powerPlayShotsAgainst, shorthandedShotsAgainst,
                  saveShotsAgainst, savePctg, evenStrengthGoalsAgainst, powerPlayGoalsAgainst,
                  shorthandedGoalsAgainst, goalsAgainst, shotsAgainst, saves,toi_real),
                ~ replace_na(., 0)))


### ------Calculate Team-Level Metrics per Game------------------------------------------------------------------------------------------------------------------------------------

cl <- makeCluster(num_cores)
registerDoParallel(cl)

###FIRST, CALCULATE ADVANCED PLAYER LEVEL METRICS (Needed for Team level metrics below)
# Function to calculate player-level advanced metrics
all_boxscore_df <- all_boxscore_df %>%
  mutate(
    # Corsi metrics
    CF = sog + blockedShots,  # Add Missed Shots if available
    CA = evenStrengthShotsAgainst + powerPlayShotsAgainst + shorthandedShotsAgainst,
    CF_Diff = CF - CA,
    
    # Fenwick metrics
    FA = CA - blockedShots,
    Fenwick_Diff = sog - FA,
    
    # Shooting and Save Percentage
    ShootingPctg =  if_else(sog > 0, goals / sog, 0),
    PDO = ShootingPctg + savePctg
  )


# Define a function to calculate team-level cumulative metrics per game
calculate_team_metrics <- function(data, window) {
  # Specify the columns to sum or average for each game-team combination
  select_cols <- c("season","game_id", "game_date", "teamId","home_id",
                   "away_id","winning_team_id","goals", "assists", "points",
                   "plusMinus", "pim", "hits", "powerPlayGoals", "sog",
                   "faceoffWinningPctg", "blockedShots", "shifts", "giveaways",
                   "takeaways", "evenStrengthShotsAgainst",
                   "powerPlayShotsAgainst", "shorthandedShotsAgainst",
                   "saveShotsAgainst", "savePctg", "evenStrengthGoalsAgainst",
                   "powerPlayGoalsAgainst", "shorthandedGoalsAgainst",
                   "goalsAgainst", "shotsAgainst", "saves", "n_shifts",
                   "med_shift_toi_per_game","avg_shift_toi_per_game","toi_real",
                   "ShootingPctg", "PDO", "Fenwick_Diff", "FA","CA", "CF",
                   "CF_Diff","home_score","away_score")
  
  # Aggregate player data to get team-level totals per game
  team_data <- data %>%
    select(all_of(select_cols)) %>%
    mutate(
      win = if_else(teamId == winning_team_id, 1, 0),
      loss = if_else(teamId != winning_team_id, 1, 0)
    ) %>%
    group_by(season, game_id, game_date, teamId) %>%
    summarise(
      # Preserve metadata for later spread calc
      home_id = first(home_id),
      away_id = first(away_id),
      home_score = first(home_score),
      away_score = first(away_score),
      
      #Advanced Metrics Calculation
      across(c(goals:toi_real), sum, na.rm = TRUE, .names = "teams_{.col}"),
      teams_ShootingPctg = sum(goals, na.rm = TRUE) / sum(sog, na.rm = TRUE),
      teams_PDO = teams_ShootingPctg + teams_savePctg,
      teams_CF = sum(sog, na.rm = TRUE) + sum(blockedShots, na.rm = TRUE),
      teams_CA = sum(evenStrengthShotsAgainst + powerPlayShotsAgainst +
                       shorthandedShotsAgainst, na.rm = TRUE),
      teams_Corsi_Diff = teams_CF - teams_CA,
      teams_FF = sum(sog, na.rm = TRUE),
      teams_FA = teams_CA - sum(blockedShots, na.rm = TRUE),
      teams_Fenwick_Diff = teams_FF - teams_FA,
      teams_win = first(win), # Results Preserve win_result
      teams_loss = first(loss)
    ) %>%
    ungroup() %>%
    # ✅ NOW calculate the spread at the team-game level
    mutate(
      teams_game_spread = case_when(teamId == home_id ~ home_score - away_score,
                                    teamId == away_id ~ away_score - home_score,
                                    TRUE ~ NA_real_)
    ) %>%
    arrange(season, game_id, game_date, teamId) %>%
    group_by(season,teamId) %>%
    mutate(
      # Step 3: Calculate cumulative sums with lag
      across(starts_with("teams_") & !ends_with("_cum_lag") & !ends_with("_roll_avg"), ~ lag(cumsum(.), default = 0),
             .names = "{.col}_cum_lag")) %>%
    select(-c("home_id", "away_id","home_score","away_score"))
  # Step 4: Calculate rolling averages with a lag, handling missing values
  for (w in window) {
    team_data <- team_data %>%
      mutate(
        across(starts_with("teams_") & !ends_with("_cum_lag") & !ends_with("_roll_avg"), ~ {
          roll_avg <- zoo::rollapply(lag(.x, 1, default = 0), width = w,
                                     FUN = mean, fill = NA, align = "right")
          if_else(is.na(roll_avg), lag(cummean(.x), default = 0), roll_avg)
        }, .names = "{.col}_{w}_roll_avg")
      )
  }
  team_data <- team_data %>%
    mutate(
      # Step 5: Calculate cumulative averages with lag
      across(starts_with("teams_") & !ends_with("_cum_lag") 
             & !ends_with("_roll_avg"), ~ lag(cummean(.), default = 0), 
             .names = "{.col}_cum_lag_avg")) %>%
    mutate(
      teams_win_prop_cum_lag = if_else((teams_win_cum_lag + teams_loss_cum_lag)
                                       > 0, teams_win_cum_lag /
                                         (teams_win_cum_lag +
                                            teams_loss_cum_lag), 0),
      
      teams_win_prop_15_roll_avg = if_else((teams_win_15_roll_avg +
                                              teams_loss_15_roll_avg)
                                           > 0, teams_win_15_roll_avg /
                                             (teams_win_15_roll_avg +
                                                teams_loss_15_roll_avg), 0),
      
      teams_win_prop_cum_lag_avg = if_else((teams_win_cum_lag_avg +
                                              teams_loss_cum_lag_avg)
                                           > 0, teams_win_cum_lag_avg /
                                             (teams_win_cum_lag_avg +
                                                teams_loss_cum_lag_avg), 0)) %>%
    ungroup()
  return(team_data)
}

windows <- c(3,7,15)
team_test  <- all_boxscore_df %>%
  filter(season == "2022")

# Apply the unified function for team metrics
team_metrics <- calculate_team_metrics(all_boxscore_df, windows)
View(team_metrics[,c("game_id", "game_date", "teamId", "teams_win",
                     "teams_loss", "teams_win_cum_lag", "teams_loss_cum_lag",
                     "teams_win_15_roll_avg", "teams_loss_15_roll_avg", "teams_win_cum_lag_avg",
                     "teams_loss_cum_lag_avg", "teams_win_prop_cum_lag",
                     "teams_win_prop_15_roll_avg", "teams_win_prop_cum_lag_avg")])
stopCluster(cl)


# Join the team-level metrics back to the main boxscore dataframe
all_boxscore_df <- all_boxscore_df %>%
  left_join(team_metrics, by = c("game_id", "game_date", "teamId", "season"))%>%
  mutate(
    opp_teamId = ifelse(teamId == home_id, away_id, home_id),
    # Add a new column to indicate if the player is on the home team
    is_home = ifelse(teamId == home_id, 1, 0),
    toi_real_lag1 = replace_na(toi_real_lag1, 0))%>%
  left_join(team_metrics,
            by = c("game_id", "game_date", "season","opp_teamId" = "teamId"),
            suffix = c("", "_opp")) %>%
  rename_with(~ sub("teams_", "team_", .))

# checkit <- all_boxscore_df_test %>% arrange(game_date,teamId)

rm(all_boxscore_df_test)
# boxscore_test <- all_boxscore_df_test %>%
#                         filter(teamId == c("8","10"), season == "2022")
# boxscore_test <-  all_boxscore_df_test %>%
#                         filter(playerId == "8473453", season == "2022")

saveRDS(all_boxscore_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))


## -----Player Metrics----------------------------------------------------------------------------------------------------------------------------------------------
num_cores <- as.numeric(detectCores())-3
cl <- makeCluster(num_cores)
registerDoParallel(cl)

rds_files_path <- getwd()
all_boxscore_df <- readRDS(paste0(rds_files_path,"/Data/combined_2009_2024_boxscore_v2.rds"))

#Advanced Player Metrics
all_boxscore_df <- all_boxscore_df %>%
  mutate(
    # Relative Corsi/Fenwick (if off-ice data is available)
    Relative_Corsi = CF_Diff - team_Corsi_Diff, # Replace mean() with off-ice calculation
    Relative_Fenwick = Fenwick_Diff - team_Fenwick_Diff # Replace mean() with off-ice calculation
  )

# Step 1: Define a function to calculate cumulative metrics for each player up to the current game
calculate_player_cumulative_metrics <- function(data,windows) {
  player_select_cols <- c("season", "game_id", "game_date", "playerId",
                          "goals", "assists", "points", "plusMinus", "pim",
                          "hits", "powerPlayGoals", "sog", "faceoffWinningPctg",
                          "blockedShots", "shifts", "giveaways", "n_shifts",
                          "med_shift_toi_per_game","avg_shift_toi_per_game",
                          "takeaways","evenStrengthShotsAgainst",
                          "powerPlayShotsAgainst", "shorthandedShotsAgainst",
                          "saveShotsAgainst","savePctg", 
                          "evenStrengthGoalsAgainst", "powerPlayGoalsAgainst", 
                          "shorthandedGoalsAgainst","goalsAgainst", 
                          "shotsAgainst","saves", "PDO", "Relative_Fenwick", 
                          "Relative_Corsi", "ShootingPctg", "Fenwick_Diff", 
                          "FA","CA", "CF", "CF_Diff")
  
  # Step 2: Prepare player-specific data with cumulative metrics for all games up to each game
  player_data <- data %>%
    select(all_of(player_select_cols)) %>%
    arrange(season, game_date,playerId) %>%
    group_by(playerId, season) %>%
    mutate(
      across(goals:shotsAgainst, ~lag(cumsum(.), default = 0),
             .names = "player_cum_lag_{.col}")
    )
  
  # Step 3: Calculate rolling averages and sums for each window separately
  for (w in windows) {
    player_data <- player_data %>%
      mutate(
        # Rolling Averages
        across(goals:takeaways, ~ {
          # Apply lag before sliding to exclude current game
          lagged_x <- lag(.x, 1, default = 0)
          # Calculate rolling average
          roll_avg <- slide_dbl(
            lagged_x,
            .f = mean,
            .before = w - 1,
            .after = 0,
            .complete = FALSE
          )
          # Replace NA with cumulative mean up to that point
          if_else(
            row_number() < w,
            cummean(lag(.x, 1, default = 0)),
            roll_avg
          )
        }, .names = "player_{.col}_roll_avg_{w}"),
        
        # Rolling Sums
        across(goals:takeaways, ~ {
          # Apply lag before sliding to exclude current game
          lagged_x <- lag(.x, 1, default = 0)
          # Calculate rolling sum
          roll_sum <- slide_dbl(
            lagged_x,
            .f = sum,
            .before = w - 1,
            .after = 0,
            .complete = FALSE
          )
          # Replace NA with cumulative sum up to that point
          if_else(
            row_number() < w,
            lag(cumsum(.x), 1, default = 0),
            roll_sum
          )
        }, .names = "player_{.col}_roll_sum_{w}")
      )
  }
  
  player_data <- player_data %>%
    select(-player_select_cols[5:length(player_select_cols)]) %>%
    ungroup()
  
  return(player_data)
}

#Test
player_cum_test <- all_boxscore_df %>%
  filter(playerId == "8473453", season == "2022")

player_test <- calculate_player_cumulative_metrics(
  player_cum_test, windows = c(3, 7, 15)) %>%
  distinct(game_id, season, game_date,
           playerId,.keep_all = TRUE)


# Step 3: Apply the function to calculate cumulative player metrics
# Apply the function
player_cumulative_metrics <- calculate_player_cumulative_metrics(
  all_boxscore_df, windows = c(3, 7, 15)) %>%
  distinct(game_id, season, game_date,
           playerId,.keep_all = TRUE)

player_cumulative_metrics <- player_cumulative_metrics %>%
  select(-matches("^player_cum_lag_(faceoffWinningPctg|plusMinus)$")) %>%
  select(-matches("^player_(faceoffWinningPctg|plusMinus)_roll_sum.*"))
stopCluster(cl)

# Step 4: Join the cumulative player metrics back to the original dataset
all_boxscore_df_test <- all_boxscore_df %>%
  left_join(player_cumulative_metrics, by =
              c("game_id", "season", "game_date", "playerId"))
all_boxscore_df <- all_boxscore_df_test
rm(all_boxscore_df_test)


## ---Cross-Season Metrics (EMA)------------------------------------------------------------------------------------------------------------------------------------------------
library(TTR)
# Example usage
# Example usage
ema_cols <- c("goals", "assists", "points", "plusMinus", "pim",
              "hits", "powerPlayGoals", "sog", "takeaways",
              "faceoffWinningPctg", "blockedShots", "shifts",
              "giveaways", "evenStrengthShotsAgainst",
              "powerPlayShotsAgainst", "shorthandedShotsAgainst",
              "saveShotsAgainst", "savePctg",
              "evenStrengthGoalsAgainst","powerPlayGoalsAgainst",
              "shorthandedGoalsAgainst","goalsAgainst",  "PDO",
              "shotsAgainst","saves", "Relative_Fenwick",
              "Relative_Corsi", "ShootingPctg","Fenwick_Diff",
              "FA","CA", "CF", "CF_Diff")

# Define a safe EMA function with fallback logic
safe_EMA <- function(x, n) {
  if(length(x) < n){
    if(length(x) == 0){
      return(rep(0, length(x)))
    } else{
      return(cummean(x))
    }
  }
  else{
    ema_val <- TTR::EMA(x, n = n)
    # Replace initial NAs (from insufficient data within the window) with cummean
    ema_val[is.na(ema_val)] <- cummean(x)[is.na(ema_val)]
    return(ema_val)
  }
}

# Define the function to calculate EMA with fallback logic
calculate_player_ema_metrics <- function(data, metrics_to_ema, ema_windows) {
  idx <- c("season", "game_id", "game_date", "playerId")
  
  # Arrange and group data by playerId and season
  player_data <- data %>%
    select(all_of(c(idx, metrics_to_ema))) %>%
    arrange(season, playerId, game_date) %>%
    group_by(playerId, season)
  
  # Apply EMA calculation for each metric and window
  for (w in ema_windows) {
    player_data <- player_data %>%
      mutate(
        across(
          all_of(metrics_to_ema),
          ~ {
            # Lag the metric to exclude the current game
            lagged_x <- lag(.x, 1, default = 0)
            
            # Apply the safe EMA function
            safe_EMA(lagged_x, w)
            
          },
          .names = "player_ema_{.col}_roll_{w}"
        )
      )
  }
  
  # Ungroup the data and remove original metrics
  player_data <- player_data %>%
    ungroup() %>%
    select(-all_of(metrics_to_ema))
  
  return(player_data)
}

# Define EMA windows and metrics
ema_windows <- c(3, 7, 15)
# Test the function with a subset of data
player_ema_test <- all_boxscore_df %>%
  filter(name.default == "A. Ovechkin") %>%
  arrange(game_date)

player_test_ema <- calculate_player_ema_metrics(player_ema_test, ema_cols,
                                                ema_windows)

player_ema_test <- player_ema_test %>%
  left_join(player_test_ema, by = c("season", "game_id", "game_date", "playerId"))

cl <- makeCluster(num_cores)
registerDoParallel(cl)
# Apply the function to the full dataset
player_ema_metrics <- calculate_player_ema_metrics(
  all_boxscore_df, ema_cols, ema_windows) %>%
  distinct(game_id, season, game_date, playerId, .keep_all = TRUE)

stopCluster(cl)

# Join EMA metrics back to the original dataset
all_boxscore_df_test <- all_boxscore_df %>%
  left_join(player_ema_metrics, by = c("season", "game_id", "game_date", "playerId"))
all_boxscore_df <- all_boxscore_df_test

rm(player_cum_test)
rm(player_cumulative_metrics)
rm(player_ema_metrics)
rm(player_ema_test)
rm(all_boxscore_df_test)
rm(player_test,team_test,player_test_ema)


## ----Drop Goalie Stats (Except for Cumulative)-----------------------------------------------------------------------------------------------------------------------------------------------
# Identify the goalie stats to remove
goalie_stats <- c("evenStrengthShotsAgainst", "powerPlayShotsAgainst", "shorthandedShotsAgainst",
                  "saveShotsAgainst", "savePctg", "evenStrengthGoalsAgainst", "powerPlayGoalsAgainst",
                  "shorthandedGoalsAgainst", "goalsAgainst", "shotsAgainst", "saves")

# Create a regex pattern to match any part of the column name
goalie_stats_pattern <- paste(goalie_stats, collapse = "|")

# Define the pattern for columns to retain (e.g., team goalie stats)
teams_lag_pattern <- "^team_.*"
cum_lag_pattern <- ".*cum_lag.*"#stationary stats are bad for model
cum_sum <- ".*cum_sum.*"
win_lag <- c("^team_win_prop_cum_lag.*")
roll_sum <- ".*roll_sum.*"

# Filter the columns
all_boxscore_df_test <- all_boxscore_df %>%
  select(
    -matches(goalie_stats_pattern), # Remove columns containing goalie stats terms
    matches(teams_lag_pattern, ignore.case = FALSE) # Retain columns matching the team pattern
  ) %>%
  select(
    -matches(cum_lag_pattern),
    matches(win_lag, ignore.case = FALSE)
  ) %>%
  select(
    -matches(roll_sum))

# Filter for goalie rows (adjust the filtering condition if your identifier is different)
goalie_df <- all_boxscore_df %>% 
  filter(main_position == "goalie") %>%
  select(
    -matches(cum_lag_pattern),
    matches(win_lag, ignore.case = FALSE)
  ) %>%
  select(
    -matches(roll_sum))

first_cols <- c("game_id", "game_date", "playerId", "teamId", "away_id", "home_id")
# Identify the goalie stat columns using your regex pattern
goalie_stat_cols <- grep(goalie_stats_pattern, names(goalie_df), value = TRUE)
# Identify any remaining columns
remaining_cols <- setdiff(names(goalie_df), c(first_cols, goalie_stat_cols))
# Reorder the dataframe: first the key columns, then the goalie stats, then all remaining columns
goalie_df <- goalie_df[, c(first_cols, goalie_stat_cols, remaining_cols)]


all_boxscore_df <- all_boxscore_df_test
rm(all_boxscore_df_test)
# rm(player_cumulative_metrics)
# rm(player_ema_metrics)
# rm(player_ema_test)
gc()


### ---Save------------------------------------------------------------------------------------------------------------------------------------------------
saveRDS(goalie_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_goalie_df_v2.rds"))
saveRDS(all_boxscore_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))
rm(goalie_df)
gc()


## -----Geographic Data--------------------------------------------------------------------------------------------------------------------------------------------
# plan(multisession, workers = 60)  # Set up parallel plan for cross-platform compatibility
library(tmaptools)
library(geosphere)

rds_files_path <- getwd()
venue_locations <- readRDS(paste0(rds_files_path, "/Data/venue_locations.rds"))
home_locations <- readRDS(paste0(rds_files_path,"/Data/home_locations.rds"))
away_locations <- readRDS(paste0(rds_files_path,"/Data/away_locations.rds"))
all_boxscore_df <- readRDS(paste0(rds_files_path,"/Data/combined_2009_2024_boxscore_v2.rds"))

#### ----Commmented code-----
# #For Troubleshooting, Save as CSVs
# write.csv(venue_locations, paste0(rds_files_path, "/Data/venue_locations.csv"))
# write.csv(home_locations,  paste0(rds_files_path,"/Data/home_locations.csv"))
# write.csv(away_locations,  paste0(rds_files_path,"/Data/away_locations.csv"))

# # Create a distinct list of venues (from the boxscore df)
# venue_locations <- all_boxscore_df %>%
#   select(venueLocation) %>%
#   distinct()
# 
# # Create a lookup table that maps "venueLocation" to "venueLocation_1"
# ven_map <- data.frame(
#   venueLocation = c(
#     "Newark","Washington","Buffalo","Montreal","New York","San Jose","Anaheim","Los Angeles",
#     "Vancouver","Chicago","Boston","Toronto","St. Louis","Uniondale","Philadelphia","Pittsburgh",
#     "Ottawa","Atlanta","Raleigh","Tampa","Sunrise","Detroit","Dallas","Columbus","Nashville",
#     "Glendale","Denver","St. Paul","Winnipeg","Calgary","Edmonton","Bronx","Brooklyn","Paradise",
#     "Gothenburg","Elmont","Seattle","Tempe","East Rutherford","Prague","Salt Lake City","Stockholm",
#     "Queens","Stateline","Foxborough","Notre Dame","Helsinki","Berlin","Hamilton","Ann Arbor",
#     "Annapolis","Tampere","Minneapolis","St. Louis ", # duplicate row
#     "Regina","Colorado Springs","Santa Clara"
#   ),
#   venueLocation_1 = c(
#     "Newark, NJ","Washington, DC","Buffalo, NY","Montreal, Quebec","New York City, NY","San Jose, CA",
#     "Anaheim, CA","Los Angeles, CA","Vancouver, BC","Chicago, IL","Boston, MA","Toronto, Ontario",
#     "St. Louis, MO","Uniondale, NY","Philadelphia, PA","Pittsburgh, PA","Ottowa, Ontario","Atlanta, GA",
#     "Raleigh, NC","Tampa Bay, FL","Sunrise, FL","Detroit, MI","Dallas, TX","Columbus, OH","Nashville, TN",
#     "Glendale, AZ","Denver, CO","St. Paul, MN","Winnipeg, Manitoba","Calgary, Alberta","Edmonton, Alberta",
#     "New York City, NY","New York City, NY","Paradise, NV","Gothenburg, Sweden","Elmont, NY","Seattle, WA",
#     "Tempe, AZ","East Rutherford, NJ","Prague, Czech Republic","Salt Lake City, UT","Stockholm, Sweden",
#     "New York City, NY","Stateline, NV","Foxborough, MA","Notre Dame, IN","Helsinki, Finland","Berlin, Germany",
#     "Hamilton, Ontario","Ann Arbor, Michigan","Annapolis, MA","Tampere, Finland","Minneapolis, MN",
#     "St. Louis, MO", # duplicate
#     "Regina, Saskatchewan","Colorado Springs, CO","Santa Clara, CA"
#   ),
#   stringsAsFactors = FALSE
# )
# 
# # Merge lookup with existing venue_locations
# venue_locations <- merge(venue_locations, ven_map, by = "venueLocation", all.x = TRUE)
# 
# # Create distinct lists of home and away teams
# home_locations <- all_boxscore_df %>%
#   select(home_team_name, home_team_locale) %>%
#   distinct()
# 
# away_locations <- all_boxscore_df %>%
#   select(away_team_name, away_team_locale) %>%
#   distinct()
# 
# # Define a lookup for (home_team_name, home_team_locale) -> venueLocation_1
# home_map <- data.frame(
#   home_team_name = c(
#     "Devils","Capitals","Sabres","Canadiens","Rangers","Sharks","Ducks","Kings","Canucks","Blackhawks",
#     "Bruins","Maple Leafs","Blues","Islanders","Flyers","Penguins","Senators","Thrashers","Hurricanes",
#     "Lightning","Panthers","Red Wings","Stars","Blue Jackets","Predators","Coyotes","Avalanche","Wild",
#     "Jets","Flames","Oilers","Coyotes","Golden Knights","Kraken","Utah Hockey Club"
#   ),
#   home_team_locale = c(
#     "New Jersey","Washington","Buffalo","Montréal","New York","San Jose","Anaheim","Los Angeles","Vancouver",
#     "Chicago","Boston","Toronto","St. Louis","New York","Philadelphia","Pittsburgh","Ottawa","Atlanta",
#     "Carolina","Tampa Bay","Florida","Detroit","Dallas","Columbus","Nashville","Phoenix","Colorado",
#     "Minnesota","Winnipeg","Calgary","Edmonton","Arizona","Vegas","Seattle","Utah"
#   ),
#   location = c(
#     "Newark, NJ","Washington, DC","Buffalo, NY","Montreal, Quebec","New York City, NY","San Jose, CA",
#     "Anaheim, CA","Los Angeles, CA","Vancouver, BC","Chicago, IL","Boston, MA","Toronto, Ontario",
#     "St. Louis, MO","Elmont, NY","Philadelphia, PA","Pittsburgh, PA","Ottowa, Ontario","Atlanta, GA",
#     "Raleigh, NC","Tampa Bay, FL","Sunrise, FL","Detroit, MI","Dallas, TX","Columbus, OH","Nashville, TN",
#     "Glendale, AZ","Denver, CO","St. Paul, MN","Winnipeg, Manitoba","Calgary, Alberta","Edmonton, Alberta",
#     "Glendale, AZ","Las Vegas, NV","Seattle, WA","Salt Lake City, UT"
#   ),
#   stringsAsFactors = FALSE
# )
# 
# # Merge onto existing home_locations
# home_locations <- merge(
#   x = home_locations,
#   y = home_map,
#   by = c("home_team_name", "home_team_locale"),
#   all.x = TRUE) %>%
#     rename(home_location = location)
# 
# 
# # For away_locations, do the same (just rename columns in the merge call)
# away_locations <- merge(
#   x = away_locations,
#   y = home_map,
#   by.x = c("away_team_name", "away_team_locale"),
#   by.y = c("home_team_name", "home_team_locale"),
#   all.x = TRUE) %>%
#     rename(away_location = location)
# 
# # Geocode venue locations
# venue_locations <- venue_locations %>%
#   mutate(
#     coords     = geo(venueLocation_1, method = "osm"),
#     venue_lat  = coords$lat,
#     venue_long = coords$long
#   ) %>%
#   select(-coords)
# 
# # Geocode home team locales
# home_locations <- home_locations %>%
#   mutate(
#     coords    = geo(home_location, method = "osm"),
#     home_lat  = coords$lat,
#     home_long = coords$long
#   ) %>%
#   select(-coords)
# 
# # Geocode away team locales
# away_locations <- away_locations %>%
#   mutate(
#     coords    = geo(away_location, method = "osm"),
#     away_lat  = coords$lat,
#     away_long = coords$long
#   ) %>%
#   select(-coords)
# 
# 
# saveRDS(venue_locations, file = paste0(rds_files_path, "/Data/venue_locations.rds"))
# saveRDS(home_locations, file = paste0(rds_files_path, "/Data/home_locations.rds"))
# saveRDS(away_locations, file = paste0(rds_files_path, "/Data/away_locations.rds"))
#### ----End Commented----

# Set up cluster
num_cores <- as.numeric(detectCores())-2
cl <- makeCluster(num_cores)
registerDoParallel(cl)



##----TRAVEL DISTANCE--------

library(dplyr)
library(geosphere)
library(zoo)

# 1. Calculate venue distances at the player level
calculate_venue_distance <- function(data) {
  data %>%
    mutate(
      home_distance = distHaversine(cbind(home_long, home_lat), cbind(venue_long, venue_lat)),
      away_distance = distHaversine(cbind(away_long, away_lat), cbind(venue_long, venue_lat))
    )
}

# 2a. Aggregate player-level distances to team-level (averaging per game)
#     Then join back so all original columns are preserved.
aggregate_team_game_distance <- function(data) {
  team_data <- data %>%
    group_by(season, game_id, game_date, teamId) %>%
    summarise(
      team_avg_home_distance = mean(home_distance, na.rm = TRUE),
      team_avg_away_distance = mean(away_distance, na.rm = TRUE),
      .groups = "drop"
    )
  left_join(data, team_data, by = c("season", "game_id", "game_date", "teamId"))
}

# 2b. Calculate team-level cumulative distance using the averaged values
calculate_cumulative_distance_team <- function(data) {
  team_cum <- data %>%
    distinct(season, game_id, game_date, teamId,
             team_avg_home_distance, team_avg_away_distance) %>%
    arrange(season, teamId, game_date) %>%
    group_by(season, teamId) %>%
    mutate(
      cum_home_distance = cumsum(team_avg_home_distance),
      cum_away_distance = cumsum(team_avg_away_distance)
    ) %>%
    ungroup()
  
  data %>%
    left_join(team_cum %>% 
                select(season, game_id, game_date, teamId, cum_home_distance, cum_away_distance),
              by = c("season", "game_id", "game_date", "teamId"))
}

# 2c. Calculate team-level rolling average distances (using a specified window)
calculate_rolling_distance_team <- function(data, window) {
  # Check if the aggregated columns exist; if not, add them
  if (!("team_avg_home_distance" %in% names(data)) ||
      !("team_avg_away_distance" %in% names(data))) {
    data <- aggregate_team_game_distance(data)
  }
  
  team_roll <- data %>%
    distinct(season, game_id, game_date,teamId,
             team_avg_home_distance,team_avg_away_distance,home_id, away_id) %>%
    arrange(season, teamId, game_date) %>%
    group_by(season, teamId) %>%
    mutate(
      cum_avg_home_distance = cummean(team_avg_home_distance),
      cum_avg_away_distance = cummean(team_avg_away_distance),
      rolling_home_distance = rollmean(team_avg_home_distance, k = window, fill = NA, align = "right"),
      rolling_away_distance = rollmean(team_avg_away_distance, k = window, fill = NA, align = "right"),
      rolling_home_distance = if_else(row_number() < window, cum_avg_home_distance, rolling_home_distance),
      rolling_away_distance = if_else(row_number() < window, cum_avg_away_distance, rolling_away_distance)
    )  %>%
    ungroup() %>%
    mutate(
      rolling_distance = if_else(teamId == home_id, rolling_home_distance, rolling_away_distance),
      rolling_distance_opp = if_else(teamId == away_id, rolling_away_distance, rolling_home_distance)) %>%
    rename_with(~ paste0(.x, "_", window), starts_with("rolling") & contains("distance"))
  
  print("joining data:")
  data %>%
    left_join(team_roll %>% 
                select(season, game_id, game_date, teamId, 
                       paste0("rolling_home_distance_", window),
                       paste0("rolling_away_distance_", window),
                       paste0("rolling_distance_", window),
                       paste0("rolling_distance_opp_", window)),
              by = c("season", "game_id", "game_date", "teamId"))
}

# 3a. For player-level rolling metrics, first compute a player's effective distance.
#     For each player, if they're on the home team use home_distance; if away, use away_distance.
calculate_player_effective_distance <- function(data) {
  data %>%
    mutate(
      player_distance = if_else(teamId == home_id, home_distance, away_distance)
    )
}

# 3b. Calculate rolling averages for player-level distance metrics.
calculate_rolling_distance_player <- function(data, window) {
  data %>%
    arrange(playerId, game_date) %>%
    group_by(season, playerId) %>%
    mutate(
      cum_avg_player_distance = cummean(player_distance),
      rolling_player_distance = rollmean(player_distance, k = window, fill = NA, align = "right"),
      rolling_player_distance = if_else(row_number() < window, cum_avg_player_distance, rolling_player_distance)
    ) %>%
    ungroup() %>%
    rename_with(~ paste0(.x, "_", window), starts_with("rolling_player_distance"))
}

# Example Pipeline:
# - First, join any location data.
# - Then compute player-level distances.
# - Next, calculate team-level averages (and subsequent cumulative/rolling team metrics).
# - Finally, compute player-level rolling metrics.
all_boxscore_df_test <- all_boxscore_df %>%
  left_join(venue_locations, by = c("venueLocation")) %>%
  left_join(away_locations, by = c("away_team_name", "away_team_locale")) %>%
  left_join(home_locations, by = c("home_team_name", "home_team_locale")) %>%
  calculate_venue_distance() %>%                              # Compute player-level distances
  ### ----- TEAM-LEVEL METRICS -----
aggregate_team_game_distance() %>%                          # Average players' distances per team per game
  calculate_cumulative_distance_team() %>%                    # Compute cumulative team-level metrics
  calculate_rolling_distance_team(window = 3) %>%             # Rolling team metrics (window=5)
  calculate_rolling_distance_team(window = 7) %>%            # Rolling team metrics (window=15)
  calculate_rolling_distance_team(window = 15) %>%            # Rolling team metrics (window=25)
  ### ----- PLAYER-LEVEL METRICS -----
calculate_player_effective_distance() %>%                   # Create a column 'player_distance'
  calculate_rolling_distance_player(window = 3) %>%           # Rolling player metrics (window=5)
  calculate_rolling_distance_player(window = 7) %>%          # Rolling player metrics (window=15)
  calculate_rolling_distance_player(window = 15) %>%           # Rolling player metrics (window=25)
  select(-c("team_avg_away_distance","team_avg_home_distance")) %>%
  mutate(
    travel_distance = if_else(teamId == home_id, home_distance, away_distance),
    travel_distance_opp = if_else(teamId == away_id, away_distance, home_distance))

#Inspect and Sanity Check
ab_df_test <- all_boxscore_df_test[,c("game_id","game_date","season", "name.default",
                                      "away_team_name","away_team_locale", 
                                      "home_team_name","home_team_locale",
                                      "teamId","home_id", "away_id","away_lat","away_long",
                                      "away_distance","cum_away_distance",
                                      "cum_home_distance", "home_distance",
                                      "player_distance",
                                      "cum_avg_player_distance", 
                                      "rolling_away_distance_3", 
                                      "rolling_away_distance_7", 
                                      "rolling_away_distance_15",
                                      "rolling_home_distance_3",
                                      "rolling_home_distance_7", 
                                      "rolling_home_distance_15",
                                      "rolling_player_distance_3_7_15",
                                      "rolling_player_distance_7_15", 
                                      "rolling_player_distance_15",
                                      "venueLocation")] %>% filter(away_team_name == "Stars") %>% arrange(game_date, season, teamId)

all_boxscore_df <- all_boxscore_df_test  %>% arrange(game_date, season, teamId)
rm(all_boxscore_df_test)
stopCluster(cl)
saveRDS(all_boxscore_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))

## --TIMEZONES-------------------------------------------------------------------------------------------------------------------------------------------

#install.packages("lutz")
#install.packages("sftime")

library(sftime)
library(dplyr)
library(lubridate)
library(lutz)
library(furrr)
library(parallel)
library(purrr)
library(future)
library(progressr)

num_cores <- detectCores()-2
# cl <- makeCluster(num_cores)
# registerDoParallel(cl)

rds_files_path <- getwd()
all_boxscore_df <- readRDS(paste0(rds_files_path,"/Data/combined_2009_2024_boxscore_v2.rds"))
# all_shift_df <- readRDS("/users/willschneider/Hockey Project/Data/combined_2009_2024_shifts_v2.rds")
# all_plays_df <- readRDS("/users/willschneider/Hockey Project/Data/combined_2009_2024_plays_v2.rds")

# Step 1: Extract unique combinations for time zone calculations
time_zone_df <- all_boxscore_df %>%
  select(game_id, game_date, home_lat, home_long, away_lat, away_long,
         venueUTCOffset) %>%
  distinct() %>%
  mutate(
    game_date = as.Date(game_date),
    home_time_zone = tz_lookup_coords(home_lat, home_long, method = "accurate"),
    away_time_zone = tz_lookup_coords(away_lat, away_long, method = "accurate")
  )
gc()

#### ----Commmented code-----
# # Step 2: Calculate offsets with future_map_dbl
# time_zone_df <- time_zone_df %>%
#   mutate(
#     home_tz_offset = future_map_dbl(home_time_zone, function(tz) {
#       if (is.na(tz)) {
#         NA_real_  # Use NA_real_ to ensure numeric NA
#       } else {
#         tz_offset(game_date, tz)[[1, 5]]
#       }
#     }),
#     away_tz_offset = future_map_dbl(away_time_zone, function(tz) {
#       if (is.na(tz)) {
#         NA_real_
#       } else {
#         tz_offset(game_date, tz)[[1, 5]]
#       }
#     })
#   )
# saveRDS(time_zone_df, file = paste0(rds_files_path, "/Data/time_zone_df.rds"))

#### ----End Commented----
plan(multisession)
# Define file path for the cached time_zone_df
tz_file <- paste0(rds_files_path, "/Data/time_zone_df.rds")

# Extract unique rows from the new data
new_tz_keys <- all_boxscore_df %>%
  select(game_id, game_date, startTimeUTC, home_lat, home_long, away_lat, away_long, venueUTCOffset) %>%
  distinct() %>%
  mutate(game_date = as.Date(game_date))

if (file.exists(tz_file)) {
  print("Cache Exists. Updating Cache")
  # Load pre-computed offsets from cache
  cached_tz <- readRDS(tz_file)
  
  # Identify new rows not in the cached data using all join keys
  new_rows <- new_tz_keys %>%
    anti_join(cached_tz, by = c("game_id", "game_date", "startTimeUTC", 
                                "home_lat", "home_long", "away_lat", "away_long", "venueUTCOffset"))
  
  if (nrow(new_rows) > 0) {
    # Create a progressor for the two mapping functions (2 steps per row)
    total_steps <- 2 * nrow(new_rows)
    handlers(global = TRUE)  # Use the default global handlers (e.g., txtprogressbar)
    
    with_progress({
      p <- progressor(steps = total_steps)
      
      new_rows <- new_rows %>%
        mutate(
          home_time_zone = tz_lookup_coords(home_lat, home_long, method = "accurate"),
          away_time_zone = tz_lookup_coords(away_lat, away_long, method = "accurate")
        ) %>%
        mutate(
          home_tz_offset = future_map2_dbl(home_time_zone, startTimeUTC, function(tz, st) {
            p()  # Increment progress
            if (is.na(tz)) NA_real_ else tz_offset(st, tz)[[1, 5]]
          }),
          away_tz_offset = future_map2_dbl(away_time_zone, startTimeUTC, function(tz, st) {
            p()  # Increment progress
            if (is.na(tz)) NA_real_ else tz_offset(st, tz)[[1, 5]]
          })
        )
    })
    
    # Append new rows to the cached data and save the updated cache
    time_zone_df <- bind_rows(cached_tz, new_rows)
    saveRDS(time_zone_df, file = tz_file)
  } else {
    # No new rows—use the cached data as is
    time_zone_df <- cached_tz
  }
  
} else {
  print("No Cache Exists. Creating Cache")
  # No cache exists, so compute everything from scratch
  # Create a progressor for both mapping functions
  new_row_count <- nrow(new_tz_keys)
  total_steps <- 2 * new_row_count
  handlers(global = TRUE)
  
  with_progress({
    p <- progressor(steps = total_steps)
    
    time_zone_df <- new_tz_keys %>%
      mutate(
        home_time_zone = tz_lookup_coords(home_lat, home_long, method = "accurate"),
        away_time_zone = tz_lookup_coords(away_lat, away_long, method = "accurate")
      ) %>%
      mutate(
        home_tz_offset = future_map2_dbl(home_time_zone, startTimeUTC, function(tz, st) {
          p()
          if (is.na(tz)) NA_real_ else tz_offset(st, tz)[[1, 5]]
        }),
        away_tz_offset = future_map2_dbl(away_time_zone, startTimeUTC, function(tz, st) {
          p()
          if (is.na(tz)) NA_real_ else tz_offset(st, tz)[[1, 5]]
        })
      )
  })
  
  saveRDS(time_zone_df, file = tz_file)
}
plan(sequential)

time_zone_df <- readRDS(paste0(rds_files_path, "/Data/time_zone_df.rds"))
time_zone_df_2 <- time_zone_df %>%
  mutate(
    venue_utc_offset = as.numeric(str_sub(venueUTCOffset, 1, 3)), # Convert venueUTCOffset to numeric offset
    home_venue_time_diff = venue_utc_offset - home_tz_offset,
    away_venue_time_diff = venue_utc_offset - away_tz_offset,
    tz_diff_game = home_tz_offset - away_tz_offset) %>%
  select( # Include these columns for joining
    game_id, game_date, startTimeUTC, home_lat, home_long, away_lat, away_long,
    venueUTCOffset, home_tz_offset, away_tz_offset, home_venue_time_diff,
    away_venue_time_diff, tz_diff_game, home_time_zone, away_time_zone
  )

all_boxscore_df <- all_boxscore_df %>%
  mutate(
    home_lat = as.numeric(as.character(home_lat)),
    home_long = as.numeric(as.character(home_long)),
    away_lat = as.numeric(as.character(away_lat)),
    away_long = as.numeric(as.character(away_long))
  )

time_zone_df_2 <- time_zone_df_2 %>%
  mutate(
    home_lat = as.numeric(as.character(home_lat)),
    home_long = as.numeric(as.character(home_long)),
    away_lat = as.numeric(as.character(away_lat)),
    away_long = as.numeric(as.character(away_long))
  )

# Step 4: Join back to main dataframe
all_boxscore_df_test <- all_boxscore_df %>%
  mutate(game_date = as.Date(game_date)) %>%
  left_join(time_zone_df_2, by = c("game_id", "game_date","startTimeUTC", "home_lat", "home_long", "away_lat", "away_long", "venueUTCOffset"))

#NA Check
rows_with_na <- all_boxscore_df_test %>%
  select(-c("decision","starter"))  %>%
  filter(if_any(everything(), is.na))

View(all_boxscore_df_test %>% 
       filter(game_id %in% unique(rows_with_na$game_id)))

View(time_zone_df %>% 
       filter(game_id %in% unique(rows_with_na$game_id)))

all_boxscore_df <- all_boxscore_df_test
rm(all_boxscore_df_test,new_tz_keys, time_zone_df, time_zone_df_2)
saveRDS(all_boxscore_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))
gc()

## ----ROLLING TZ DIFFS---------------------------------------------------------------------------------------------------------------------------------------
library(profvis)
library(furrr)
library(parallel)
library(purrr)

# Set up parallel processing with an optimal number of workers
num_cores <- detectCores()-2
plan(multisession, workers = num_cores)

calculate_rolling_timezone_diff_single <- function(data, window=5) {
  data %>%
    arrange(teamId, game_date) %>%
    group_by(teamId, season) %>%
    mutate(
      # Calculate cumulative absolute and average time zone differences
      cumulative_abs_tz_diff = cumsum(tz_diff_game),
      cumulative_avg_tz_diff = cumsum(tz_diff_game) / row_number(),
      # Calculate rolling metrics for a single window
      rolling_abs_tz_diff = slide_dbl(abs(tz_diff_game), sum, .before = window - 1, .complete = TRUE),
      rolling_avg_tz_diff = slide_dbl(tz_diff_game, mean, .before = window - 1, .complete = TRUE),
      # Apply conditional logic for rows with insufficient data
      rolling_abs_tz_diff = if_else(row_number() < window, cumulative_abs_tz_diff, rolling_abs_tz_diff),
      rolling_avg_tz_diff = if_else(row_number() < window, cumulative_avg_tz_diff, rolling_avg_tz_diff)) %>%
    
    mutate(cumulative_abs_tz_diff = coalesce(cumulative_abs_tz_diff,0),
           cumulative_avg_tz_diff = coalesce(cumulative_avg_tz_diff,0),
           rolling_abs_tz_diff = coalesce(rolling_abs_tz_diff,0),
           rolling_avg_tz_diff = coalesce(rolling_avg_tz_diff,0)) %>%
    rename_with(~ paste0(.x, "_",window), starts_with("rolling") & ends_with("tz_diff")) %>%
    ungroup()
}

start_t <-  Sys.time()
# Step 6: Apply the rolling function with a window of your choice (e.g., 5 games)
all_boxscore_df<- all_boxscore_df %>%
  calculate_rolling_timezone_diff_single(window = 3) %>%
  calculate_rolling_timezone_diff_single(window = 7) %>%
  calculate_rolling_timezone_diff_single(window = 15)

end_t <- Sys.time()
end_t - start_t

plan(sequential)

saveRDS(all_boxscore_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))


## ----HURST EXPONENT-----------------------------------------------------------------------------------------------------------------------------------------------
# library(dplyr)
# library(zoo)
# library(pracma)
# library(foreach)
# library(doParallel)
# library(lubridate) # For handling date intervals
# 
# # Load data
# rds_files_path <- getwd()
# all_boxscore_df <- readRDS(paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))
# 
# # Split data by playerId and season for independent parallel processing
# split_data_list <- all_boxscore_df %>%
#   group_split(playerId, season)
# 
# # Set up cluster
# num_cores <- detectCores()-2
# cl <- makeCluster(num_cores)
# registerDoParallel(cl)
# 
# # Define the Hurst calculation function using He only
# hurst_calculation <- function(series) {
#   # Remove NA values
#   series <- na.omit(series)
# 
#   # Check if the series is long enough
#   if (length(series) < 10) {
#     return(NA)
#   }
# 
#   # Check for constant series
#   if (length(unique(series)) <= 1) {
#     return(NA)
#   }
# 
#   # # Difference the series to make it stationary
#   # diff_series <- diff(series)
#   #
#   # # After differencing, check again
#   # if (length(unique(diff_series)) <= 1) {
#   #   return(NA)
#   # }
# 
#   # Calculate Hurst exponent using 'He' only
#   hurst_values <- tryCatch(
#     hurstexp(series, display = FALSE),
#     error = function(e) return(NA)
#   )
# 
#   # Return 'He' if available
#   if (!is.na(hurst_values$Hal)) {
#     return(hurst_values$Hal)
#   } else {
#     return(NA)
#   }
# }
# 
# # Define the Adjusted Bridge Range function
# adjusted_bridge_range <- function(series, scale) {
#   # Apply rollapply with the specified scale
#   rollapply(series, width = scale, function(x) {
#     # Remove NA values
#     x <- na.omit(x)
# 
#     # Ensure sufficient data points
#     if (length(x) < scale) return(NA)
# 
#     # Compute base range
#     base_range <- max(x, na.rm = TRUE) - min(x, na.rm = TRUE)
# 
#     # Calculate Hurst exponent on the window
#     h_val <- hurst_calculation(x)
#     if (is.na(h_val)) return(NA)
# 
#     # Compute run-length encoding of trend directions
#     trend_run <- rle(sign(diff(x)))$lengths
#     max_run_length <- ifelse(length(trend_run) > 0, max(trend_run, na.rm = TRUE), NA)
# 
#     # Apply bridge range adjustment based on run length
#     if (!is.na(max_run_length) && max_run_length > 2) {
#       return(base_range ^ h_val)
#     } else {
#       return(base_range)
#     }
#   }, align = "right", fill = NA, partial = FALSE)
# }
# 
# # Define a function that applies calculations to one subset
# process_subset <- function(subset_df, scales) {
#   # Arrange and group data
#   subset_df <- subset_df %>%
#     arrange(playerId, game_date) %>%
#     group_by(playerId, season) %>%
#     # Create add_subtract metric: +1 if points > 0, -1 otherwise
#     mutate(
#       add_subtract_points = ifelse(points > 0, 1, -1),
#       # Lag the metrics to prevent data leakage
#       points_lag = lag(points, 1, default = 0),
#       add_subtract_points_lag = lag(add_subtract_points, 1, default = 0)
#     ) %>%
#     ungroup()
# 
#   # Define the metrics to be used for Hurst calculation
#   metrics <- list(
#     list(name = "points", column = "points_lag"),
#     list(name = "add_subtract", column = "add_subtract_points_lag")
#   )
# 
#   # Iterate over each scale and metric to compute Hurst and Adjusted Bridge Range
#   # Implement fallback logic
#   for (metric in metrics) {
#     for (scale in scales) {
#       hurst_col <- paste0("rolling_hurst_", metric$name, "_", scale)
#       adj_bridge_col <- paste0("rolling_adjusted_bridge_range_", metric$name, "_", scale)
# 
#       # Initialize new columns as NA
#       subset_df[[hurst_col]] <- NA
#       subset_df[[adj_bridge_col]] <- NA
# 
#       subset_df <- subset_df %>%
#         arrange(playerId, game_date) %>%
#         group_by(playerId, season) %>%
#         mutate(
#           !!hurst_col := rollapply(
#             .data[[metric$column]],
#             width = scale,
#             FUN = hurst_calculation,
#             align = "right",
#             fill = NA,
#             partial = FALSE
#           ),
#           !!adj_bridge_col := adjusted_bridge_range(.data[[metric$column]], scale)
#           ) %>%
#         ungroup()
# 
#       if (scale == 25) {
#         subset_df <- subset_df %>%
#           arrange(playerId, game_date) %>%
#           group_by(playerId, season) %>%
#           mutate(
#             !!hurst_col := ifelse(
#               is.na(.data[[hurst_col]]),
#               ifelse(
#                 !is.na(.data[[paste0("rolling_hurst_", metric$name, "_", 15)]]),
#                 .data[[paste0("rolling_hurst_", metric$name, "_", 15)]],
#                 ifelse(
#                   !is.na(.data[[paste0("rolling_hurst_", metric$name, "_", 10)]]),
#                   .data[[paste0("rolling_hurst_", metric$name, "_", 10)]],
#                   0
#                 )
#               ),
#               .data[[hurst_col]]
#             ),
#             !!adj_bridge_col := ifelse(
#               is.na(.data[[adj_bridge_col]]),
#               ifelse(
#                 !is.na(.data[[paste0("rolling_adjusted_bridge_range_", metric$name, "_", 15)]]),
#                 .data[[paste0("rolling_adjusted_bridge_range_", metric$name, "_", 15)]],
#                 ifelse(
#                   !is.na(.data[[paste0("rolling_adjusted_bridge_range_", metric$name, "_", 10)]]),
#                   .data[[paste0("rolling_adjusted_bridge_range_", metric$name, "_", 10)]],
#                   0
#                 )
#               ),
#               .data[[adj_bridge_col]]
#             )
#           ) %>%
#           ungroup()
# 
#         # Log if fallback occurred
#         fallback_occurred <- subset_df %>%
#           filter(is.na(.data[[paste0("rolling_hurst_", metric$name, "_", scale)]])) %>%
#           pull(playerId)
# 
#         if (length(fallback_occurred) > 0) {
#           message(paste("Fallback applied for playerId(s):", paste(fallback_occurred, collapse = ", "), "on scale:", scale))
#         }
#       }
# 
#       if (scale == 15) {
#         subset_df <- subset_df %>%
#           arrange(playerId, game_date) %>%
#           group_by(playerId, season) %>%
#           mutate(
#             !!hurst_col := ifelse(
#               is.na(.data[[hurst_col]]),
#               ifelse(
#                 !is.na(.data[[paste0("rolling_hurst_", metric$name, "_", 10)]]),
#                 .data[[paste0("rolling_hurst_", metric$name, "_", 10)]],
#                 0
#               ),
#               .data[[hurst_col]]
#             ),
#             !!adj_bridge_col := ifelse(
#               is.na(.data[[adj_bridge_col]]),
#               ifelse(
#                 !is.na(.data[[paste0("rolling_adjusted_bridge_range_", metric$name, "_", 10)]]),
#                 .data[[paste0("rolling_adjusted_bridge_range_", metric$name, "_", 10)]],
#                 0
#               ),
#               .data[[adj_bridge_col]]
#             )
#           ) %>%
#           ungroup()
# 
#         # Log if fallback occurred
#         fallback_occurred <- subset_df %>%
#           filter(is.na(.data[[paste0("rolling_hurst_", metric$name, "_", scale)]])) %>%
#           pull(playerId)
# 
#         if (length(fallback_occurred) > 0) {
#           message(paste("Fallback applied for playerId(s):", paste(fallback_occurred, collapse = ", "), "on scale:", scale))
#         }
#       }
# 
#       if (scale == 10) {
#         subset_df <- subset_df %>%
#           arrange(playerId, game_date) %>%
#           group_by(playerId, season) %>%
#           mutate(
#             !!hurst_col := ifelse(is.na(.data[[hurst_col]]), 0, .data[[hurst_col]]),
#             !!adj_bridge_col := ifelse(is.na(.data[[adj_bridge_col]]), 0, .data[[adj_bridge_col]])
#           ) %>%
#           ungroup()
# 
#         # Log if fallback occurred
#         fallback_occurred <- subset_df %>%
#           filter(is.na(.data[[paste0("rolling_hurst_", metric$name, "_", scale)]])) %>%
#           pull(playerId)
# 
#         if (length(fallback_occurred) > 0) {
#           message(paste("Fallback applied for playerId(s):", paste(fallback_occurred, collapse = ", "), "on scale:", scale))
#         }
#       }
#     }
#   }
# 
#   return(subset_df)
# }
# 
# # Define scales
# scales <- c(10, 15, 25)
# 
# # Start parallel processing
# start_time <- Sys.time()
# results_list <- foreach(df_chunk = split_data_list,
#                         .packages = c("dplyr", "zoo", "pracma", "lubridate")) %dopar% {
#   process_subset(df_chunk, scales)
# }
# stopCluster(cl)
# end_time <- Sys.time()
# 
# # results <- all_boxscore_df %>% filter(name.default == "A. Ovechkin")
# # num_cores <- as.numeric(Sys.getenv("SLURM_CPUS_PER_TASK", unset=4))-1
# # cl <- makeCluster(num_cores)
# # registerDoParallel(cl)
# # hurst_df_test <- process_subset(results,scales)
# # stopCluster(cl)
# # saveRDS(all_boxscore_df_test, file = paste0(rds_files_path, "/combined_2009_2024_boxscore_v2.rds"))
# 
# # Combine results into a single data frame
# all_boxscore_df_test <- bind_rows(results_list)
# hurst_time <- end_time - start_time
# hurst_time
# # Handle NA Hurst values by carrying forward the last known Hurst value within each group
# all_boxscore_df_test <- all_boxscore_df_test %>%
#   group_by(playerId, season) %>%
#   mutate(across(starts_with("rolling_hurst_"), ~ zoo::na.locf(.x, na.rm = FALSE))) %>%
#   ungroup()
# 
# all_boxscore_df <- all_boxscore_df_test
# # Save the updated data frame
# saveRDS(all_boxscore_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))
# 
# # Save execution time to a file
# write(paste("Execution Time:", hurst_time), paste0(rds_files_path,"/execution_time_hurst.txt"))
# rm(all_boxscore_df_test,results_list, split_data_list)
# gc()

## ----Final NA Check-----------------------------------------------------------------------------------------------------------------------------------------------
library(naniar)
library(tidymodels)
library(tidyverse)
library(dplyr)
library(zoo)

rds_files_path <- getwd()
all_boxscore_df <- readRDS(paste0(rds_files_path,"/Data/combined_2009_2024_boxscore_v2.rds"))

# #summary of NA counts
total_rows <- nrow(all_boxscore_df)
na_summary <- all_boxscore_df %>%
  summarise(across(everything(), ~sum(is.na(.)) /
                     total_rows * 100, .names = "na_percentage_{col}")) %>%
  pivot_longer(cols = everything(),
               names_to = "column", values_to = "na_percentage") %>%
  arrange(desc(na_percentage))
na_summary


#Inspect Missing data
# Identify columns with all NA values
rows_with_na <- all_boxscore_df %>%
  select(-c("decision","starter")) %>%
  filter(if_any(everything(), is.na))

View(rows_with_na %>% filter(game_status == "unplayed"))
gg_miss_var(all_boxscore_df %>% filter(game_status == "played"))$data  # Missing data count by variable
gg_miss_upset(all_boxscore_df %>% filter(game_status == "played"))  # Visualize missingness patterns


# --Final Preprocessing-------
rds_files_path <- getwd()
all_boxscore_df <- readRDS(paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))
# Set up cluster
num_cores <- detectCores()-2
cl <- makeCluster(num_cores)
registerDoParallel(cl)


#### Additional Feature Engineering
##### Rolling TOI Windows
# Function to calculate rolling average of 'real_toi' with lag

calculate_rolling_avg_toi_with_lag <- function(data, window_size) {
  data %>%
    arrange(playerId, season, game_date) %>%
    group_by(playerId, season) %>%
    mutate(
      rolling_avg_toi_lag = lag(rollmean(toi_real, k = window_size, fill = NA), n = 1)
    ) %>%
    ungroup()
}

# Calculate rolling averages with lag
all_boxscore_df <- all_boxscore_df %>%
  calculate_rolling_avg_toi_with_lag(3) %>%
  rename(rolling_avg_toi_3d_lag = rolling_avg_toi_lag) %>%
  calculate_rolling_avg_toi_with_lag(7) %>%
  rename(rolling_avg_toi_7d_lag = rolling_avg_toi_lag) %>%
  calculate_rolling_avg_toi_with_lag(15) %>%
  rename(rolling_avg_toi_15d_lag = rolling_avg_toi_lag)

# Calculate lagged cumulative average
all_boxscore_df <- all_boxscore_df %>%
  arrange(playerId, season, game_date) %>%
  group_by(playerId, season) %>%
  mutate(cumulative_avg_toi_lag = lag(cummean(toi_real), n = 1)) %>%
  mutate(
    rolling_avg_toi_3d_lag = coalesce(rolling_avg_toi_3d_lag, cumulative_avg_toi_lag, 0),
    rolling_avg_toi_7d_lag = coalesce(rolling_avg_toi_7d_lag, cumulative_avg_toi_lag, 0),
    rolling_avg_toi_15d_lag = coalesce(rolling_avg_toi_15d_lag, cumulative_avg_toi_lag, 0)) %>%
  ungroup()

# Function to calculate rolling taxing factor with lag
calculate_taxing_factor_with_lag <- function(data, window_size) {
  data %>%
    arrange(playerId, season, game_date) %>%
    group_by(playerId, season) %>%
    mutate(
      rolling_sum_toi = rollsum(toi_real, k = window_size, fill = NA, align = "right"),
      rolling_sum_rest = rollsum(player_hrs_since_last_game, k = window_size, fill = NA, align = "right"),
      taxing_factor = lag(rolling_sum_toi / rolling_sum_rest)  # Lagged taxing factor
    ) %>%
    ungroup()
}

# Apply the function to create taxing factors for different windows
all_boxscore_df <- all_boxscore_df %>%
  calculate_taxing_factor_with_lag(3) %>%
  rename(taxing_factor_3d_lag = taxing_factor) %>%
  calculate_taxing_factor_with_lag(7) %>%
  rename(taxing_factor_7d_lag = taxing_factor) %>%
  calculate_taxing_factor_with_lag(15) %>%
  rename(taxing_factor_15d_lag = taxing_factor)

# Replace missing taxing factors with meaningful defaults
all_boxscore_df <- all_boxscore_df %>%
  mutate(
    taxing_factor_3d_lag = coalesce(taxing_factor_3d_lag, 0),
    taxing_factor_7d_lag = coalesce(taxing_factor_7d_lag, taxing_factor_3d_lag, 0),
    taxing_factor_15d_lag = coalesce(taxing_factor_15d_lag, taxing_factor_7d_lag, taxing_factor_3d_lag, 0)
  )

# Step 1: Ensure 'teamId' and 'home_id' are the same type for comparison
all_boxscore_df <- all_boxscore_df %>%
  mutate(
    teamId = as.character(teamId),
    home_id = as.character(home_id),
    away_id = as.character(away_id)
  )

# Step 2: Filter out rows with excessive NAs or irrelevant columns
all_boxscore_df <- all_boxscore_df  %>%
  select(-c(starter,decision,cumulative_avg_toi_lag, toi_real_lag1,
            rolling_sum_toi, rolling_sum_rest))%>%
  mutate(
    home_hrs_since_last_game = coalesce(home_hrs_since_last_game, 0),
    # Replace NA with 0 or other value
    away_hrs_since_last_game = coalesce(away_hrs_since_last_game, 0),
    player_hrs_since_last_game = coalesce(player_hrs_since_last_game, 0)
  )


###################################################################

gg_miss_var(all_boxscore_df)$data
tryCatch({
  gg_miss_upset(all_boxscore_df)
}, error = function(e) {
  message("An error occurred: ", e$message)
})

# Step 4: Create a binary variable 'is_home' based on teamId match
all_boxscore_df <- all_boxscore_df %>%
  mutate(is_home = if_else(teamId == home_id, 1, 0))

##### ONLY USE THIS CODE IF PREPROCESSING DURING A LIVE GAME ######
all_boxscore_df <- all_boxscore_df %>%
  arrange(playerId, game_date) %>%  # sort by player and game date
  group_by(playerId) %>%
  fill(everything(), .direction = "down") %>%
  ungroup()

stopCluster(cl)
roll_sum <- ".*roll_sum.*"

# Filter the columns
all_boxscore_df <- all_boxscore_df %>%
  select( #Remove Roll Sum and player rolls (replaced with EMAs)
    -matches(".*roll_sum.*")) %>%
  select(-matches("player.*roll_avg", ignore.case = TRUE))


all_boxscore_df <- all_boxscore_df %>%
  arrange(season, game_date, playerId) %>%
  mutate(
    earned_point = if_else(points > 0, 1, 0),
    earned_goal = if_else(goals > 0, 1, 0),
    earned_assist = if_else(assists > 0, 1, 0),
  )

saveRDS(all_boxscore_df, file = paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))

#--- FINAL PREPROCESSING AND BASIC RECIPE  -----
library(tidymodels)
library(zoo)
library(tidyverse)
library(glmnet)
library(tictoc)
library(finetune)
library(naniar)
library(doParallel)
library(geosphere)
library(dplyr)
library(purrr)
library(themis)

set.seed(123)

rds_files_path <- getwd()
all_boxscore_df <- readRDS(paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))

# Create a new outcome variable "game_won"
player_df <- all_boxscore_df
rm(all_boxscore_df)

player_df <- player_df %>%
  mutate(earned_point = factor(earned_point, levels = c("1", "0")),
         earned_goal = factor(earned_goal, levels = c("1", "0")),
         earned_assist = factor(earned_assist, levels = c("1", "0"))) %>%
  filter(!(main_position == "goalie")) %>%
  filter(season >= "2019")

team_df <- player_df %>%
  group_by(game_id, game_date, teamId) %>%
  summarise(across(everything(), ~ first(.))) %>%
  ungroup() %>% 
  select(-matches("player_|_player_")) %>% 
  select(-c("earned_goal","earned_assist","earned_point")) %>%
  mutate(
    game_won = factor(
      if_else(
        (teamId == home_id & home_score > away_score) |
          (teamId == away_id & away_score > home_score), "1", "0"),
      levels = c("1", "0")
    ),
    game_won_spread = factor(
      if_else(
        (team_game_spread > 1.5), "1", "0"),
      levels = c("1", "0")
    )
  ) 

#CHECK - Game outcomes (win/loss) should be equal
outcome_check <- team_df %>% filter(game_status == "played") %>%
  group_by(game_id) %>%
  summarise(
    wins = sum(game_won == "1", na.rm = TRUE),
    losses = sum(game_won == "0", na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(wins != 1 | losses != 1)
print(outcome_check)

problem_games <- outcome_check %>% pull(game_id)
problem_details <- team_df %>% filter(game_id %in% problem_games)
print(problem_details)

count(team_df %>% filter(game_status == "played") %>% filter(game_won == "1"))
count(team_df %>% filter(game_status == "played") %>% filter(game_won == "0"))

# Function to calculate rolling team-level metrics with lag
calculate_team_rolling_metrics_with_lag <- function(data, window_size) {
  data %>%
    arrange(teamId, season, game_date, startTimeUTC, game_id) %>%
    group_by(teamId, season, game_date, startTimeUTC, game_id) %>%
    summarise(
      median_toi = median(toi_real, na.rm = TRUE),
      # iqr_toi = IQR(toi_real, na.rm = TRUE),
      top75_toi = quantile(toi_real, 0.75, na.rm = TRUE),
      median_rest = median(player_hrs_since_last_game, na.rm = TRUE),
      iqr_rest = IQR(player_hrs_since_last_game, na.rm = TRUE),
      top75_rest = quantile(player_hrs_since_last_game, 0.75, na.rm = TRUE)
    ) %>%
    arrange(teamId, season, game_date, startTimeUTC) %>%
    group_by(teamId, season) %>%
    mutate(
      team_roll_median_toi_lag = lag(rollapply(median_toi, window_size, mean, fill = NA, align = "right")),
      # team_roll_iqr_toi_lag = lag(rollapply(iqr_toi, window_size, mean, fill = NA, align = "right")),
      team_roll_top75_toi_lag = lag(rollapply(top75_toi, window_size, mean, fill = NA, align = "right")),
      team_roll_median_rest_lag = lag(rollapply(median_rest, window_size, mean, fill = NA, align = "right")),
      # team_roll_iqr_rest_lag = lag(rollapply(iqr_rest, window_size, mean, fill = NA, align = "right")),
      team_roll_top75_rest_lag = lag(rollapply(top75_rest, window_size, mean, fill = NA, align = "right"))
    ) %>%
    ungroup() %>%
    select(teamId, season, game_date, game_id, startTimeUTC, starts_with("team_roll"))
}

# Calculate rolling metrics at team level for 3, 7, and 15-game windows
team_metrics_df <- calculate_team_rolling_metrics_with_lag(player_df, 3) %>%
  rename_with(~ paste0(., "_3d"), starts_with("team_roll")) %>%
  left_join(
    calculate_team_rolling_metrics_with_lag(player_df, 7) %>%
      rename_with(~ paste0(., "_7d"), starts_with("team_roll")),
    by = c("teamId", "season", "game_date", "game_id","startTimeUTC")
  ) %>%
  left_join(
    calculate_team_rolling_metrics_with_lag(player_df, 15) %>%
      rename_with(~ paste0(., "_15d"), starts_with("team_roll")),
    by = c("teamId", "season", "game_date", "game_id","startTimeUTC")
  )

# Calculate lagged cumulative medians for default filling
team_metrics_df <- team_metrics_df %>%
  arrange(teamId, season, game_id, game_date, startTimeUTC) %>%
  group_by(teamId, season) %>%
  mutate(across(starts_with("team_roll"), ~coalesce(.,
                                                    lag(cummean(replace_na(., 0)), n = 1), 0))) %>%
  ungroup()


team_df_joined <- team_df %>%
  left_join(team_metrics_df, by = c("game_id", "teamId","game_date","season", "startTimeUTC"))
team_df <- team_df_joined
rm(team_metrics_df,team_df_joined)

# List of columns to remove
columns_to_remove <- c( "goals", "assists", "points", "plusMinus", "pim", 
                        "hits", "powerPlayGoals", "sog","faceoffWinningPctg",
                        "blockedShots", "shifts", "giveaways", "takeaways", "otInUse", "league",
                        "shootoutInUse","toi", "toi_real", "home_score", "team_goals",
                        "away_score", "med_shift_toi_per_game", 
                        "avg_shift_toi_per_game", "away_lat", "away_long", "home_lat", "home_long",
                        "venue_lat", "venue_long", "sweaterNumber", "cumulative_toi",
                        "name.default",  "easternUTCOffset", "team_assists", 
                        "team_plusMinus", "team_pim", "team_hits",
                        "team_powerPlayGoals", "team_sog", "team_faceoffWinningPctg",
                        "team_blockedShots", "team_shifts", "team_giveaways",
                        "team_takeaways", "team_loss", "team_assists_opp",
                        "team_pim_opp", "team_hits_opp", "team_powerPlayGoals_opp",
                        "team_sog_opp", "team_faceoffWinningPctg_opp", "team_blockedShots_opp",
                        "team_shifts_opp", "team_giveaways_opp", "team_takeaways_opp",
                        "team_loss_opp","team_points_opp", "team_plusMinus_opp",
                        "team_toi_real_opp","team_win_opp", "team_toi_real", "team_win",
                        "team_goals_opp", "otInUse", "shootoutInUse","team_points",
                        "cumulative_abs_tz_diff", "cum_home_distance", "cum_away_distance",
                        "player_cum_lag_assists", "player_cum_lag_points","player_cum_lag_pim",
                        "player_cum_lag_hits", "player_cum_lag_powerPlayGoals", "player_cum_lag_sog",
                        "player_cum_lag_blockedShots", "player_cum_lag_shifts",
                        "player_cum_lag_giveaways", "player_cum_lag_takeaways",
                        "team_goals_cum_lag_opp", "team_assists_cum_lag_opp", "player_cum_lag_goals",
                        "team_points_cum_lag_opp", "team_plusMinus_cum_lag_opp",
                        "team_pim_cum_lag_opp", "team_hits_cum_lag_opp",
                        "team_powerPlayGoals_cum_lag_opp", "team_sog_cum_lag_opp",
                        "team_faceoffWinningPctg_cum_lag_opp", "team_blockedShots_cum_lag_opp",
                        "team_shifts_cum_lag_opp", "team_giveaways_cum_lag_opp",
                        "team_takeaways_cum_lag_opp", "team_toi_real_cum_lag_opp",
                        "team_win_cum_lag_opp", "team_loss_cum_lag_opp", "team_goals_cum_lag_avg_opp",
                        "team_assists_cum_lag_avg_opp", "team_points_cum_lag_avg_opp",
                        "team_plusMinus_cum_lag_avg_opp", "team_pim_cum_lag_avg_opp",
                        "team_hits_cum_lag_avg_opp", "team_powerPlayGoals_cum_lag_avg_opp",
                        "team_sog_cum_lag_avg_opp" ,"team_faceoffWinningPctg_cum_lag_avg_opp",
                        "team_blockedShots_cum_lag_avg_opp" ,"team_shifts_cum_lag_avg_opp",
                        "team_giveaways_cum_lag_avg_opp" ,"team_takeaways_cum_lag_avg_opp",
                        "team_toi_real_cum_lag_avg_opp" ,"team_win_cum_lag_avg_opp",
                        "lagged_cum_shift_toi_last_X_games",
                        "team_loss_cum_lag_avg_opp" ,"team_win_prop_cum_lag_opp",
                        "team_win_prop_10_roll_avg_opp" ,"team_win_prop_cum_lag_avg_opp",
                        "team_goals_cum_lag_avg","team_assists_cum_lag_avg",
                        "team_points_cum_lag_avg", "team_plusMinus_cum_lag_avg",
                        "team_pim_cum_lag_avg", "team_hits_cum_lag_avg",
                        "team_powerPlayGoals_cum_lag_avg", "team_sog_cum_lag_avg" ,
                        "team_faceoffWinningPctg_cum_lag_avg", "team_blockedShots_cum_lag_avg" ,
                        "team_shifts_cum_lag_avg", "team_giveaways_cum_lag_avg" ,
                        "team_takeaways_cum_lag_avg",  "team_toi_real_cum_lag_avg" ,
                        "team_win_cum_lag_avg", "team_loss_cum_lag_avg" ,"team_win_prop_cum_lag",
                        "team_win_prop_10_roll_avg" ,"team_win_prop_cum_lag_avg",
                        "cum_avg_home_distance" ,"cum_avg_away_distance","team_goals_cum_lag",
                        "team_assists_cum_lag", "team_points_cum_lag", "team_plusMinus_cum_lag",
                        "team_pim_cum_lag",  "team_hits_cum_lag", "team_powerPlayGoals_cum_lag",
                        "team_sog_cum_lag","team_faceoffWinningPctg_cum_lag",
                        "team_blockedShots_cum_lag","team_shifts_cum_lag", "team_giveaways_cum_lag",
                        "team_takeaways_cum_lag",  "team_toi_real_cum_lag","team_win_cum_lag",
                        "team_loss_cum_lag","cumulative_avg_tz_diff",
                        "player_goals_roll_sum_5", "player_assists_roll_sum_5",
                        "player_points_roll_sum_5","player_pim_roll_sum_5","player_hits_roll_sum_5",
                        "player_powerPlayGoals_roll_sum_5","player_sog_roll_sum_5",
                        "player_blockedShots_roll_sum_5", "player_shifts_roll_sum_5",
                        "player_giveaways_roll_sum_5","player_takeaways_roll_sum_5",
                        "player_goals_roll_sum_15", "player_assists_roll_sum_15",
                        "player_points_roll_sum_15","player_pim_roll_sum_15",
                        "player_hits_roll_sum_15", "player_powerPlayGoals_roll_sum_15",
                        "player_sog_roll_sum_15", "player_blockedShots_roll_sum_15",
                        "player_shifts_roll_sum_15","player_giveaways_roll_sum_15",
                        "player_takeaways_roll_sum_15","player_goals_roll_sum_30",
                        "player_assists_roll_sum_30","player_points_roll_sum_30",
                        "player_pim_roll_sum_30","player_hits_roll_sum_30",
                        "player_powerPlayGoals_roll_sum_30", "player_sog_roll_sum_30",
                        "player_blockedShots_roll_sum_30","player_shifts_roll_sum_30",
                        "player_giveaways_roll_sum_30","player_takeaways_roll_sum_30",
                        "lag_adjusted_bridge_range_cumulative_points_5",
                        "lag_adjusted_bridge_range_cumulative_avg_5",
                        "lag_adjusted_bridge_range_simple_add_subtract_5",
                        "lag_adjusted_bridge_range_cumulative_avg_15",
                        "lag_adjusted_bridge_range_cumulative_points_15",
                        "lag_adjusted_bridge_range_simple_add_subtract_15",
                        "lag_hurst_cumulative_avg_15","lag_hurst_simple_add_subtract_15",
                        "lag_hurst_cumulative_avg_30", "lag_hurst_cumulative_avg_5",
                        "lag_cum_avg_points", "lag_adjusted_bridge_range_cumulative_points_30",
                        "lag_adjusted_bridge_range_cumulative_avg_30",
                        "lag_adjusted_bridge_range_simple_add_subtract_30",
                        "lag_hurst_simple_add_subtract_5", "lag_hurst_simple_add_subtract_30",
                        "lag_hurst_cumulative_points_5","lag_hurst_cumulative_points_15",
                        "lag_hurst_cumulative_points_30","Relative_Corsi", "Relative_Fenwick",
                        "team_evenStrengthShotsAgainst", "team_powerPlayShotsAgainst",
                        "team_shorthandedShotsAgainst", "team_saveShotsAgainst", "team_savePctg",
                        "team_evenStrengthGoalsAgainst", "team_powerPlayGoalsAgainst",
                        "team_shorthandedGoalsAgainst", "team_goalsAgainst", "team_shotsAgainst",
                        "team_saves", "rolling_abs_tz_diff_5", "rolling_abs_tz_diff_15",
                        "rolling_abs_tz_diff_30", "add_subtract_points", "points_lag", "CA",
                        "CF_Diff", "FA", "Fenwick_Diff", "ShootingPctg", "PDO", "team_ShootingPctg",
                        "team_PDO", "team_CF", "team_CA", "team_Corsi_Diff", "team_FF", "team_FA",
                        "team_Fenwick_Diff","CF","team_ShootingPctg_opp", "team_PDO_opp",
                        "team_CF_opp", "team_CA_opp", "team_Corsi_Diff_opp", "team_FF_opp",
                        "team_FA_opp", "team_Fenwick_Diff_opp", "team_evenStrengthShotsAgainst_opp",
                        "team_powerPlayShotsAgainst_opp", "team_shorthandedShotsAgainst_opp",
                        "team_saveShotsAgainst_opp", "team_savePctg_opp",
                        "team_evenStrengthGoalsAgainst_opp", "team_powerPlayGoalsAgainst_opp",
                        "team_shorthandedGoalsAgainst_opp", "team_goalsAgainst_opp",
                        "team_shotsAgainst_opp", "team_saves_opp", "add_subtract_points",
                        "points_lag", "add_subtract_points_lag", "rolling_hurst_points_10",
                        "rolling_adjusted_bridge_range_points_10", "rolling_hurst_add_subtract_10",
                        "rolling_adjusted_bridge_range_add_subtract_10", "CA",
                        "rolling_adjusted_bridge_range_add_subtract_15",
                        "rolling_adjusted_bridge_range_add_subtract_25", 
                        "rolling_adjusted_bridge_range_points_10", "player_ema_CA_roll_5",
                        "player_ema_CA_roll_15", "player_ema_CA_roll_25",
                        "rolling_adjusted_bridge_range_points_10",
                        "rolling_adjusted_bridge_range_points_15",
                        "rolling_adjusted_bridge_range_points_25", "cumulative_avg_toi_lag",
                        "toi_real_lag1", "lagged_med_shift_toi","lagged_med_shift_toi_last_X_games",
                        "lagged_avg_shift_toi_last_X_games", "rolling_sum_toi", "rolling_sum_rest",
                        "cum_home_distance","cum_away_distance", "home_time_zone", "away_time_zone",
                        "venueLocation_1","away_location","home_location","rolling_hurst_points_15", 
                        "rolling_hurst_points_25", "rolling_hurst_add_subtract_15",
                        "rolling_hurst_add_subtract_25", "cum_avg_player_distance" ,"player_distance",
                        "cum_avg_player_distance","rolling_hurst_points_10",
                        "rolling_adjusted_bridge_range_points_10", "rolling_hurst_points_15",
                        "rolling_adjusted_bridge_range_points_15", "rolling_hurst_points_25",
                        "rolling_adjusted_bridge_range_points_25", "rolling_hurst_add_subtract_10",
                        "rolling_adjusted_bridge_range_add_subtract_10", 
                        "rolling_hurst_add_subtract_15","rolling_hurst_add_subtract_25",
                        "rolling_adjusted_bridge_range_add_subtract_15", 
                        "rolling_adjusted_bridge_range_add_subtract_25","n_shifts",
                        "rolling_avg_toi_3d_lag","rolling_avg_toi_5d_lag",
                        "rolling_avg_toi_10d_lag", "taxing_factor_3d_lag",
                        "taxing_factor_5d_lag", "taxing_factor_10d_lag", 
                        "team_n_shifts","team_med_shift_toi_per_game",
                        "team_avg_shift_toi_per_game","team_n_shifts_opp",
                        "team_med_shift_toi_per_game_opp",
                        "team_avg_shift_toi_per_game_opp","main_position",
                        "team_game_spread","teams_game_spread", "position",
                        "teams_win", "teams_loss", "team_win","team_loss",
                        "team_game_spread_opp", "lagged_avg_shift_toi", 
                        "lagged_n_shift", "lagged_game_toi", 
                        "lagged_avg_n_shift_last_X_games")

columns_to_remove <- unique(columns_to_remove)
common_columns <- intersect(columns_to_remove, colnames(team_df))
rm(all_boxscore_df)

# Ensure player_df is sorted by season and game_date
team_df <- team_df %>%
  arrange(season, startTimeUTC) %>%
  mutate(game_won = as.factor(game_won)) %>%
  drop_na() %>%
  select(-all_of(common_columns))

team_df <- team_df %>%
  arrange(teamId, season, startTimeUTC) %>%
  group_by(teamId, season) %>%
  mutate(
    game_won_numeric = as.integer(as.character(game_won)),
    days_since_last_game = as.integer(as.Date(startTimeUTC) - lag(as.Date(startTimeUTC))),
    is_back_to_back = if_else(days_since_last_game == 1, 1, 0),
    is_back_to_back = coalesce(is_back_to_back,0)
  ) %>%
  mutate(
    b2b_win = if_else(is_back_to_back == 1, game_won_numeric, NA_integer_)
  ) %>%
  # Calculate win rate only on back-to-back second games
  mutate(
    b2b_wins_cumsum = cumsum(replace_na(b2b_win, 0)),
    b2b_games_cumsum = cumsum(!is.na(b2b_win)),
    b2b_win_rate = if_else(b2b_games_cumsum > 0, b2b_wins_cumsum / b2b_games_cumsum, 0)
  ) %>%
  # Carry forward last known win rate and lag it for prospective use
  fill(b2b_win_rate, .direction = "down") %>%
  mutate(b2b_win_ratio_lag = lag(b2b_win_rate, 1),
         b2b_win_ratio_lag = coalesce(b2b_win_ratio_lag,0)) %>%
  ungroup() %>%
  select(-game_won_numeric, -b2b_win, -b2b_wins_cumsum, -b2b_games_cumsum, -b2b_win_rate, -days_since_last_game)

rm(player_df)
saveRDS(team_df,file = paste0(rds_files_path, "/Data/team_df_v2.rds"))
team_df <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds"))
team_df_played <- team_df %>% filter(game_status == "played")


### ----ROLLING CV (250 GAME SPLIT)-----
#Create a game-level data frame
game_level_df <- team_df_played %>%
  distinct(game_id, game_date, startTimeUTC) %>%
  arrange(startTimeUTC, game_id) %>%
  mutate(game_index = row_number())

# Create rolling origin resamples at the game level
game_splits <- rolling_origin(
  data = game_level_df,
  initial = 3611,   # Approx. _ season
  assess = 250,      # Approx. _ games in the test set
  cumulative = FALSE,
  skip = 250       # No overlap between test sets
)

# Translate splits to player level
translate_splits <- function(spl) {
  train_games <- analysis(spl)$game_id
  test_games <- assessment(spl)$game_id
  
  # Ensure same-date games are handled correctly
  train_dates <- game_level_df %>%
    filter(game_id %in% train_games) %>%
    pull(startTimeUTC)
  
  test_dates <- game_level_df %>%
    filter(game_id %in% test_games) %>%
    pull(startTimeUTC)
  
  #find player rows that match those training or test dates
  train_indices <- which(team_df_played$startTimeUTC %in% train_dates)
  test_indices <- which(team_df_played$startTimeUTC %in% test_dates)
  
  rsample::make_splits(
    list(analysis = train_indices, assessment = test_indices),
    data = team_df_played
  )
}

# Set up parallel backend
num_cores <- detectCores()
cl <- makeCluster(max(0,num_cores-2))
registerDoParallel(cl)

# Step 1: Translate all game-level splits into player-level splits
team_splits_list <- map(game_splits$splits, translate_splits)

# Step 4: Create the rset object for the remaining splits
team_splits <- rsample::manual_rset(
  splits = team_splits_list,
  ids = game_splits$id
)
final_split <- team_splits$splits[[dim(team_splits)[1]]]
team_splits <- team_splits[-dim(team_splits)[1],]

# Ensure team_splits is a valid rset object
class(team_splits) <- c("manual_rset", "rset", "tbl_df", "tbl", "data.frame")
rm(team_splits_list)

for (s in 1:dim(team_splits)[1]) {
  #Check logic working
  first_split <- team_splits$splits[[s]]
  train_indices <- analysis(first_split)
  test_indices <- assessment(first_split)
  
  print(paste0("NA's in ",s,"train split: ",sum(colSums(is.na(train_indices)))))
  print(paste0("NA's in ",s,"test split: ",sum(colSums(is.na(test_indices)))))
  
}

rm(train_indices)
rm(test_indices)
rm(first_split)

# Define recipe, model, and workflow
team_recipe <- recipe(game_won ~ ., data = team_df_played) %>%
  step_rm(game_status) %>%
  step_rm(game_won_spread) %>%
  step_rm(playerId) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id"))) %>%
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, opp_teamId,
              new_role = "ID") %>%
  update_role(game_date, new_role = "DATE") %>%
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  step_mutate(is_home = as.factor(is_home)) %>%
  step_mutate(season = as.factor(season)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel(all_nominal_predictors(), -is_home) %>%
  step_dummy(all_nominal_predictors())

vars <- team_recipe$var_info
rec_bake <- team_recipe %>% prep() %>% bake(., new_data =  NULL)

stopCluster(cl)
gc()


saveRDS(team_df_played, file = paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
saveRDS(team_recipe, file = paste0(rds_files_path, "/Data/team_recipe_goal_v2.rds"))
saveRDS(final_split, file = paste0(rds_files_path, "/Data/team_final_split.rds"))
saveRDS(team_splits, file = paste0(rds_files_path, "/Data/team_splits.rds"))
rm(team_recipe, final_split, team_splits,game_splits)
gc()

### ----ROLLING CV (25 GAME SPLIT)-----
# Create a game-level data frame
game_level_df <- team_df_played %>%
  distinct(game_id, game_date, startTimeUTC) %>%
  arrange(startTimeUTC, game_id) %>%
  mutate(game_index = row_number())

# Create rolling origin resamples at the game level
game_splits <- rolling_origin(
  data = game_level_df,
  initial = 3611,   # Approx. _ season
  assess = 25,      # Approx. _ games in the test set
  cumulative = FALSE,
  skip = 25       # No overlap between test sets
)

# Translate splits to player level
translate_splits <- function(spl) {
  train_games <- analysis(spl)$game_id
  test_games <- assessment(spl)$game_id
  
  # Ensure same-date games are handled correctly
  train_dates <- game_level_df %>%
    filter(game_id %in% train_games) %>%
    pull(startTimeUTC)
  
  test_dates <- game_level_df %>%
    filter(game_id %in% test_games) %>%
    pull(startTimeUTC)
  
  #find player rows that match those training or test dates
  train_indices <- which(team_df_played$startTimeUTC %in% train_dates)
  test_indices <- which(team_df_played$startTimeUTC %in% test_dates)
  
  rsample::make_splits(
    list(analysis = train_indices, assessment = test_indices),
    data = team_df_played
  )
}

# Set up parallel backend
num_cores <- detectCores()
cl <- makeCluster(max(0,num_cores-2))
registerDoParallel(cl)

# Step 1: Translate all game-level splits into player-level splits
team_splits_list <- map(game_splits$splits, translate_splits)

# Step 4: Create the rset object for the remaining splits
team_splits <- rsample::manual_rset(
  splits = team_splits_list,
  ids = game_splits$id
)
final_split <- team_splits$splits[[dim(team_splits)[1]]]
team_splits <- team_splits[-dim(team_splits)[1],]

# Ensure team_splits is a valid rset object
class(team_splits) <- c("manual_rset", "rset", "tbl_df", "tbl", "data.frame")
rm(team_splits_list)

for (s in 1:dim(team_splits)[1]) {
  #Check logic working
  first_split <- team_splits$splits[[s]]
  train_indices <- analysis(first_split)
  test_indices <- assessment(first_split)
  
  print(paste0("NA's in ",s,"train split: ",sum(colSums(is.na(train_indices)))))
  print(paste0("NA's in ",s,"test split: ",sum(colSums(is.na(test_indices)))))
  
}

rm(train_indices)
rm(test_indices)
rm(first_split)

saveRDS(final_split, file = paste0(rds_files_path, "/Data/team_final_split_v2.rds"))
saveRDS(team_splits, file = paste0(rds_files_path, "/Data/team_splits_v2.rds"))

stopCluster(cl)
rm(team_df_played,team_splits,final_split)
gc()

## -----Define Spread Prediction Recipe-------
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))

#Define recipe, model, and workflow
team_recipe <- recipe(game_won_spread ~ ., data = team_df_played) %>%
  step_rm(game_status) %>%
  step_rm(game_won) %>%
  step_rm(playerId) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id"))) %>%
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, opp_teamId,
              new_role = "ID") %>%
  update_role(game_date, new_role = "DATE") %>%
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  step_mutate(is_home = as.factor(is_home)) %>%
  step_mutate(season = as.factor(season)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel(all_nominal_predictors(), -is_home) %>%
  step_dummy(all_nominal_predictors())

vars <- team_recipe$var_info
rec_bake <- team_recipe %>% prep() %>% bake(., new_data =  NULL)

gc()

saveRDS(team_recipe, file = paste0(rds_files_path, "/Data/team_recipe_spread.rds"))
rm(team_df_played,team_recipe)

library(tidyverse)

# View class distribution
team_df_played %>%
  count(game_won_spread) %>%
  mutate(prop = n / sum(n))

# Bar plot
team_df_played %>%
  ggplot(aes(x = game_won_spread)) +
  geom_bar(fill = "steelblue") +
  labs(title = "Class Distribution", x = "Outcome", y = "Count") +
  theme_minimal()
