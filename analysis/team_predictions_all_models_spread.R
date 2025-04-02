# ---- LOAD LIBRARIES ----------------------------------------------------------------------
library(tidyverse)
library(lubridate)
library(tidymodels)

# ---- PATHS AND FILES ---------------------------------------------------------------------
rds_files_path <- getwd()
combined_log_path <- file.path(rds_files_path, "Data/team_unplayed_combined_predictions_spread.csv")

# ---- STEP 1: COMBINE MODEL PREDICTION LOGS -----------------------------------------------
# Load individual logs with model identifiers
logit_log <- read_csv(file.path(rds_files_path, "Data/team_unplayed_predictions_spread_glm.csv")) %>%
  mutate(model = "logistic") %>% select(-c("playerId","winning_team_id", "winning_team")) %>%
  mutate(american_odds = ifelse(pred_probability< 0.5, ((1 - pred_probability)/pred_probability) * 100, -(pred_probability/(1- pred_probability))*100)) %>%
  mutate(cal_american_odds = ifelse(cal_probability< 0.5, ((1 - cal_probability)/cal_probability) * 100, -(cal_probability/(1- cal_probability))*100)) %>%
  select(
    game_id, game_date, teamId, venueLocation, venueUTCOffset, startTimeUTC,
    gameType, home_id, away_id, home_team_name, away_team_name, 
    home_team_locale, away_team_locale, season, game_status, pred_class, 
    pred_probability, american_odds, cal_probability, cal_american_odds, 
    prediction_time, model_version, actual_outcome, model, everything())  #%>%
  # rename(glm_pred_probability = pred_probability, glm_pred_class = pred_class, 
         # glm_cal_probability = cal_probability, 
         # glm_prediction_time = prediction_time)

rf_log <- read_csv(file.path(rds_files_path, "Data/team_unplayed_predictions_spread_rf.csv")) %>%
  mutate(model = "random forest") %>% select(-c("playerId","winning_team_id", "winning_team")) %>%
  mutate(american_odds = ifelse(pred_probability< 0.5, ((1 - pred_probability)/pred_probability) * 100, -(pred_probability/(1- pred_probability))*100)) %>%
  mutate(cal_american_odds = ifelse(cal_probability< 0.5, ((1 - cal_probability)/cal_probability) * 100, -(cal_probability/(1- cal_probability))*100)) %>%
  select(
    game_id, game_date, teamId, venueLocation, venueUTCOffset, startTimeUTC,
    gameType, home_id, away_id, home_team_name, away_team_name, 
    home_team_locale, away_team_locale, season, game_status, pred_class, 
    pred_probability, american_odds, cal_probability, cal_american_odds, 
    prediction_time, model_version, actual_outcome, model, everything())  #%>%
# rename(glm_pred_probability = pred_probability, glm_pred_class = pred_class, 
# glm_cal_probability = cal_probability, 
# glm_prediction_time = prediction_time)

mlp_log <- read_csv(file.path(rds_files_path, "Data/team_unplayed_predictions_spread_mlp.csv")) %>%
  mutate(model = "mlp") %>% select(-c("playerId","winning_team_id", "winning_team")) %>%
  mutate(american_odds = ifelse(pred_probability< 0.5, ((1 - pred_probability)/pred_probability) * 100, -(pred_probability/(1- pred_probability))*100)) %>%
  mutate(cal_american_odds = ifelse(cal_probability< 0.5, ((1 - cal_probability)/cal_probability) * 100, -(cal_probability/(1- cal_probability))*100)) %>%
  select(
    game_id, game_date, teamId, venueLocation, venueUTCOffset, startTimeUTC,
    gameType, home_id, away_id, home_team_name, away_team_name, 
    home_team_locale, away_team_locale, season, game_status, pred_class, 
    pred_probability, american_odds, cal_probability, cal_american_odds, 
    prediction_time, model_version, actual_outcome, model, everything())  #%>%
# rename(glm_pred_probability = pred_probability, glm_pred_class = pred_class, 
# glm_cal_probability = cal_probability, 
# glm_prediction_time = prediction_time)

# Standardize timestamp format and add current prediction_time if missing
add_timestamp_if_missing <- function(df) {
  if (!"prediction_time" %in% names(df)) {
    df <- df %>% mutate(prediction_time = Sys.time())
  } else {
    df <- df %>% mutate(prediction_time = ymd_hms(prediction_time))
  }
  return(df)
}

logit_log <- add_timestamp_if_missing(logit_log)
glmnet_log <- add_timestamp_if_missing(rf_log)
mlp_log <- add_timestamp_if_missing(mlp_log)

# Combine all predictions into one log
combined_log <- bind_rows(logit_log, rf_log, mlp_log) %>%
  arrange(game_id, game_date, startTimeUTC, model)

# Save unified prediction log
write_csv(combined_log, combined_log_path)

# ---- STEP 2: UPDATE WITH ACTUAL OUTCOMES -------------------------------------------------
# Load actual outcomes
team_df <- readRDS(file.path(rds_files_path, "Data/team_df_v2.rds"))

# Join actual outcomes to completed games in the combined log
updated_combined_log <- combined_log %>%
  mutate(teamId = as.character(teamId),
         pred_class = as.character(pred_class),
         game_date = as.Date(game_date),
         home_id = as.character(combined_log$home_id),
         away_id = as.character(combined_log$away_id),
         startTimeUTC = as.character(combined_log$startTimeUTC),
         prediction_time = as_datetime(combined_log$prediction_time)) %>%
  left_join(team_df %>% select(game_id, teamId, game_won, startTimeUTC),
            by = c("game_id", "teamId","startTimeUTC")) %>%
  mutate(game_time = as.POSIXct(startTimeUTC, tz = "America/Chicago"),
    eligible_for_update = Sys.time() > (game_time + days(1)),
    actual_outcome = if_else(eligible_for_update, game_won, NA)
  ) %>%
  select(-startTimeUTC, -game_time, -game_won, -eligible_for_update)

# Save updated log
write_csv(updated_combined_log, combined_log_path)
