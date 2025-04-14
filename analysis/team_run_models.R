## ----- WIN GAME - LOGISTIC - PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(tidymodels)
library(doParallel)
library(themis)
library(glmnet)
library(dplyr)
library(purrr)
library(finetune)
library(probably)
library(betacal)
library(themis)

# Assume you already have these objects saved from your prior work:
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
team_wf_log <- readRDS(paste0(rds_files_path,"/Data/team_wf_log_v2.rds"))

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Fit your workflow on all training data for spread models -------------
# This creates a fully fitted model using all of team_df_played
final_model <- fit(team_wf_log, data = team_df_played)

train_preds <- predict(final_model, new_data = team_df_played, type = "prob")
train_class <- predict(final_model, new_data = team_df_played) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(team_df_played %>% select(game_won))

cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won,
                                estimate = .pred_1)

team_df_cal_preds <- train_model %>% cal_apply(cv_cal_mod)
saveRDS(team_df_cal_preds, file = paste0(rds_files_path, "/Data/team_deployable_model_preds_glm.rds"))

rm(train_preds, train_class)

### -----Step 2: Refit final model on correct training window-----
team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3611) %>%
  pull(game_id)

# Filter training rows from team_df_played
train_df <- team_df_played %>%
  filter(game_id %in% team_df_train)
#Sanity check
length(unique(train_df$game_id))
final_model <- fit(team_wf_log, data = train_df)
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_glm.rds"))

train_preds <- predict(final_model, new_data = train_df, type = "prob")
train_class <- predict(final_model, new_data = train_df) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(train_df %>% select(game_won))

cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won,
                                estimate = .pred_1)

### ---# Step 3: Predict on Unplayed Games Using the Fully Fitted Model and----
### ----Preprocess the unplayed games data-----------------
# Since final_model is a fully fitted workflow, you can call predict() directly on new data.
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds")) %>% filter(game_status == "unplayed")
unplayed_predictions <- predict(final_model, new_data = unplayed_games, type = "prob")
unplayed_class <- predict(final_model, new_data = unplayed_games)
unplayed_cal_preds <- unplayed_predictions  %>% cal_apply(cv_cal_mod)
stopCluster(cl)

# Combine predictions with unplayed games for further use (e.g., logging, analysis)
unplayed_results <- unplayed_games %>%
  mutate(pred_probability = unplayed_predictions$.pred_1,
         pred_class = unplayed_class$.pred_class,
         cal_probability = unplayed_cal_preds$.pred_1,
         cal_class = ifelse(cal_probability > .50, 1, 0),
         prediction_time = Sys.time(),
         model_version = "glm_v_25",   # define this near the top of your script
         # Initially, actual outcome is unknown
         actual_outcome = NA) %>%
  select(-"game_won")


# Save predictions
saveRDS(unplayed_results, file = paste0(rds_files_path, "/Data/team_unplayed_games_predictions_v2.rds"))
saveRDS(unplayed_cal_preds, file = paste0(rds_files_path, "/Data/team_unplayed_games_preds_cal_v2.rds"))
rm(unplayed_class, unplayed_cal_preds, unplayed_predictions,final_model)


#### -----Update Deployment Log------
log_file <- paste0(rds_files_path, "/Data/team_unplayed_predictions_glm.csv")

# Define key columns and prediction columns
key_cols <- c("game_id", "teamId")
pred_cols <- c("pred_probability", "pred_class", "cal_probability")

if (file.exists(log_file)) {
  # Read the existing log
  old_log <- read.csv(log_file, stringsAsFactors = FALSE)
  # Match column types between new and old data (especially pred_class)
  old_log$teamId <- as.character(old_log$teamId)
  unplayed_results$teamId <- as.character(unplayed_results$teamId)
  old_log$pred_class <- as.character(old_log$pred_class)
  unplayed_results$pred_class <- as.character(unplayed_results$pred_class)
  old_log$game_date <- as.Date(old_log$game_date)
  unplayed_results$game_date <- as.Date(unplayed_results$game_date)
  old_log$home_id <- as.character(old_log$home_id)
  unplayed_results$home_id <- as.character(unplayed_results$home_id)
  old_log$away_id <- as.character(old_log$away_id)
  unplayed_results$away_id <- as.character(unplayed_results$away_id)
  old_log$prediction_time <- as_datetime(old_log$prediction_time)
  unplayed_results$prediction_time <- as_datetime(unplayed_results$prediction_time)
  old_log$game_won_spread <- as.factor(old_log$game_won_spread)
  unplayed_results$game_won_spread <- as.factor(unplayed_results$game_won_spread)
  
  # # Keep rows from new predictions that are different on keys and prediction columns
  # new_to_add <- unplayed_results %>%
  #   # Use a join that compares both keys and prediction columns
  #   anti_join(old_log, by = c(key_cols, pred_cols))
  # 
  # # Append only the new/changed rows to the existing log
  # updated_log <- bind_rows(old_log, new_to_add)
  updated_log <- bind_rows(old_log, unplayed_results)
  # Write the updated log back to file
  write.csv(updated_log, log_file, row.names = FALSE)
} else {
  # If no log exists, write unplayed_results as the new log
  write.csv(unplayed_results, log_file, row.names = FALSE)
}

## ----LOGISTIC - GAME SPREAD - PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(tidymodels)
library(doParallel)
library(themis)
library(glmnet)
library(dplyr)
library(purrr)
library(finetune)
library(probably)
library(betacal)
library(themis)

# Assume you already have these objects saved from your prior work:
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_glm.rds"))
team_wf_log <- readRDS(paste0(rds_files_path,"/Data/team_wf_log_spread.rds"))

team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3611) %>%
  pull(game_id)

# Filter training rows from team_df_played
train_df <- team_df_played %>%
  filter(game_id %in% team_df_train)
#Sanity check
length(unique(train_df$game_id))

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Fit your workflow on all training data for deployment-------------
# This creates a fully fitted model using all of team_df_played
final_model <- fit(team_wf_log, data = train_df)
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_spread.rds"))
rm(team_wf_log)

train_preds <- predict(final_model, new_data = train_df, type = "prob")
train_class <- predict(final_model, new_data = train_df) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(train_df %>% select(game_won_spread))
cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won_spread,
                                estimate = .pred_1)

rm(team_df_played, train_preds, train_class,train_df)

### -----Step 2: Preprocess the unplayed games data-----------------
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds")) %>% 
  filter(game_status == "unplayed") %>% 
  select(-game_won_spread)
unplayed_won_preds <- readRDS(paste0(rds_files_path, 
                                     "/Data/team_unplayed_games_predictions_v2.rds"))
# unplayed_won_cal <- readRDS(paste0(rds_files_path,  "/Data/team_unplayed_games_preds_cal_v2.rds"))

unplayed_games <- unplayed_games  %>% 
  left_join(unplayed_won_preds[,c("game_id","teamId","cal_probability")], 
            by = c("game_id", "teamId")) %>%
  mutate(cal_class = ifelse(cal_probability > .50, "1", "0")) %>%
  rename(game_won_.pred_1 = cal_probability, 
         favorite = cal_class) %>%
  mutate(favorite = factor(favorite,levels = c("1","0"))) %>%
  mutate(game_won_.pred_0 = 1 - game_won_.pred_1)

### ---# Step 3: Predict on Unplayed Games Using the Fully Fitted Model----
# Since final_model is a fully fitted workflow, you can call predict() directly on new data.
unplayed_predictions <- predict(final_model, new_data = unplayed_games, type = "prob")
unplayed_class <- predict(final_model, new_data = unplayed_games)
unplayed_cal_preds <- unplayed_predictions  %>% cal_apply(cv_cal_mod)
stopCluster(cl)

# Combine predictions with unplayed games for further use (e.g., logging, analysis)
unplayed_results <- unplayed_games %>%
  mutate(pred_probability = unplayed_predictions$.pred_1,
         pred_class = unplayed_class$.pred_class,
         cal_probability = unplayed_cal_preds$.pred_1,
         cal_class = ifelse(cal_probability > .50, 1, 0),
         prediction_time = Sys.time(),
         model_version = "glm_v_25_pred_with_gamewon",   # define this near the top of your script
         # Initially, actual outcome is unknown
         actual_outcome = NA)

# Save predictions
saveRDS(unplayed_results, file = paste0(rds_files_path, "/Data/team_unplayed_games_predictions_spread.rds"))
saveRDS(unplayed_cal_preds, file = paste0(rds_files_path, "/Data/team_unplayed_games_preds_cal_spread.rds"))
rm(unplayed_class, unplayed_cal_preds, unplayed_predictions,final_model)


#### -----Update Deployment Log------
log_file <- paste0(rds_files_path, "/Data/team_unplayed_predictions_spread_glm.csv")

# Define key columns and prediction columns
key_cols <- c("game_id", "teamId")
pred_cols <- c("pred_probability", "pred_class", "cal_probability")

if (file.exists(log_file)) {
  # Read the existing log
  old_log <- read.csv(log_file, stringsAsFactors = FALSE)
  # Match column types between new and old data (especially pred_class)
  old_log$teamId <- as.character(old_log$teamId)
  unplayed_results$teamId <- as.character(unplayed_results$teamId)
  old_log$pred_class <- as.character(old_log$pred_class)
  unplayed_results$pred_class <- as.character(unplayed_results$pred_class)
  old_log$game_date <- as.Date(old_log$game_date)
  unplayed_results$game_date <- as.Date(unplayed_results$game_date)
  old_log$home_id <- as.character(old_log$home_id)
  unplayed_results$home_id <- as.character(unplayed_results$home_id)
  old_log$away_id <- as.character(old_log$away_id)
  unplayed_results$away_id <- as.character(unplayed_results$away_id)
  old_log$prediction_time <- as_datetime(old_log$prediction_time)
  unplayed_results$prediction_time <- as_datetime(unplayed_results$prediction_time)
  old_log$game_won <- as.factor(old_log$game_won)
  unplayed_results$game_won <- as.factor(unplayed_results$game_won)
  unplayed_results$favorite <- as.factor(unplayed_results$favorite)
  old_log$favorite <- as.factor(old_log$favorite)
  # # Keep rows from new predictions that are different on keys and prediction columns
  # new_to_add <- unplayed_results %>%
  #   # Use a join that compares both keys and prediction columns
  #   anti_join(old_log, by = c(key_cols, pred_cols))
  # 
  # # Append only the new/changed rows to the existing log
  # updated_log <- bind_rows(old_log, new_to_add)
  updated_log <- bind_rows(old_log, unplayed_results)
  # Write the updated log back to file
  write.csv(updated_log, log_file, row.names = FALSE)
} else {
  # If no log exists, write unplayed_results as the new log
  write.csv(unplayed_results, log_file, row.names = FALSE)
}

## ----RANDOM FOREST - WIN GAME - PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(tidymodels)
library(doParallel)
library(themis)
library(glmnet)
library(dplyr)
library(purrr)
library(finetune)
library(probably)
library(betacal)
library(themis)
library(ranger)

# Assume you already have these objects saved from your prior work:
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
final_rf_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_rf.rds"))

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Fit your workflow on all training data for deployment-------------
# 1. Create a fully fitted model using all of team_df_played
final_model <- fit(final_rf_wf, data = team_df_played)
train_preds <- predict(final_model, new_data = team_df_played, type = "prob")
train_class <- predict(final_model, new_data = team_df_played) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(team_df_played %>% select(game_won))
cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won,
                                estimate = .pred_1)

team_df_cal_preds <- train_model %>% cal_apply(cv_cal_mod)
saveRDS(team_df_cal_preds, file = paste0(rds_files_path, "/Data/team_deployable_model_preds_rf.rds"))
rm(train_preds, train_class)

### -----Step 2: Refit final model on correct training window-----
team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3611) %>%
  pull(game_id)

# Filter training rows from team_df_played
team_df_played <- team_df_played %>%
  filter(game_id %in% team_df_train)
#Sanity check
length(unique(team_df_played$game_id))
# Optionally, save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_rf.rds"))
rm(final_rf_wf)

### ---# Step 3: Predict on Unplayed Games Using the Fully Fitted Model and----
### ----Preprocess the unplayed games data-----------------
train_preds <- final_model %>% predict(team_df_played, type = "prob")
train_class <- predict(final_model, new_data = team_df_played) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(team_df_played %>% select(game_won))
cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won,
                                estimate = .pred_1)

rm(team_df_played, train_preds, train_class)
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds")) %>% 
  filter(game_status == "unplayed")
# Since final_model is a fully fitted workflow, you can call predict() directly on new data.
unplayed_predictions <- predict(final_model, new_data = unplayed_games, type = "prob")
unplayed_class <- predict(final_model, new_data = unplayed_games)
unplayed_cal_preds <- unplayed_predictions  %>% cal_apply(cv_cal_mod)
stopCluster(cl)

# Combine predictions with unplayed games for further use (e.g., logging, analysis)
unplayed_results <- unplayed_games %>%
  mutate(pred_probability = unplayed_predictions$.pred_1,
         pred_class = unplayed_class$.pred_class,
         cal_probability = unplayed_cal_preds$.pred_1,
         cal_class = ifelse(cal_probability > .50, 1, 0),
         prediction_time = Sys.time(),
         model_version = "rf_v_25",   # define this near the top of your script
         # Initially, actual outcome is unknown
         actual_outcome = NA) %>%
  select(-"game_won")

# Save predictions
saveRDS(unplayed_results, file = paste0(rds_files_path, "/Data/team_unplayed_games_rf_predictions.rds"))
saveRDS(unplayed_cal_preds, file = paste0(rds_files_path, "/Data/team_unplayed_games_rf_preds_cal.rds"))
rm(unplayed_class, unplayed_cal_preds, unplayed_predictions,final_model)


#### -----Update Deployment Log------
log_file <- paste0(rds_files_path, "/Data/team_unplayed_rf_predictions.csv")

# Define key columns and prediction columns
key_cols <- c("game_id", "teamId")
pred_cols <- c("pred_probability", "pred_class", "cal_probability")

if (file.exists(log_file)) {
  # Read the existing log
  old_log <- read.csv(log_file, stringsAsFactors = FALSE)
  # Match column types between new and old data (especially pred_class)
  old_log$teamId <- as.character(old_log$teamId)
  unplayed_results$teamId <- as.character(unplayed_results$teamId)
  old_log$pred_class <- as.character(old_log$pred_class)
  unplayed_results$pred_class <- as.character(unplayed_results$pred_class)
  old_log$game_date <- as.Date(old_log$game_date)
  unplayed_results$game_date <- as.Date(unplayed_results$game_date)
  old_log$home_id <- as.character(old_log$home_id)
  unplayed_results$home_id <- as.character(unplayed_results$home_id)
  old_log$away_id <- as.character(old_log$away_id)
  unplayed_results$away_id <- as.character(unplayed_results$away_id)
  old_log$prediction_time <- as_datetime(old_log$prediction_time)
  unplayed_results$prediction_time <- as_datetime(unplayed_results$prediction_time)
  old_log$game_won_spread <- as.factor(old_log$game_won_spread)
  unplayed_results$game_won_spread <- as.factor(unplayed_results$game_won_spread)
  # # Keep rows from new predictions that are different on keys and prediction columns
  # new_to_add <- unplayed_results %>%
  #   # Use a join that compares both keys and prediction columns
  #   anti_join(old_log, by = c(key_cols, pred_cols))
  # 
  # # Append only the new/changed rows to the existing log
  # updated_log <- bind_rows(old_log, new_to_add)
  updated_log <- bind_rows(old_log, unplayed_results)
  # Write the updated log back to file
  write.csv(updated_log, log_file, row.names = FALSE)
} else {
  # If no log exists, write unplayed_results as the new log
  write.csv(unplayed_results, log_file, row.names = FALSE)
}

## ----RANDOM FOREST - GAME SPREAD - PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(tidymodels)
library(doParallel)
library(themis)
library(glmnet)
library(dplyr)
library(purrr)
library(finetune)
library(probably)
library(betacal)
library(themis)
library(ranger)

# Assume you already have these objects saved from your prior work:
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_rf.rds"))
final_rf_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_rf_spread.rds"))

team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3611) %>%
  pull(game_id)

# Filter training rows from team_df_played
train_df <- team_df_played %>%
  filter(game_id %in% team_df_train)
#Sanity check
length(unique(train_df$game_id))


num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Fit your workflow on all training data for deployment-------------
# 1. Create a fully fitted model using all of team_df_played
final_model <- fit(final_rf_wf, data = train_df)

# Optionally, save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_rf_spread.rds"))
rm(final_rf_wf)

# 2. Predict on the training data
train_preds <- final_model %>% predict(train_df, type = "prob")
train_class <- predict(final_model, new_data = train_df) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(train_df %>% select(game_won_spread))

# 3. Calibrate on full training data
cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won_spread,
                                estimate = .pred_1)
rm(team_df_played, train_preds, train_class)

### -----Step 2: Preprocess the unplayed games data-----------------
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds")) %>%
  filter(game_status == "unplayed") %>% select(-game_won_spread)
unplayed_won_preds <- readRDS(paste0(rds_files_path, "/Data/team_unplayed_games_rf_predictions.rds"))
# unplayed_won_cal <- readRDS(paste0(rds_files_path,  "/Data/team_unplayed_games_preds_cal_v2.rds"))

unplayed_games <- unplayed_games  %>% 
  left_join(unplayed_won_preds[,c("game_id","teamId","cal_probability")], 
            by = c("game_id", "teamId")) %>%
  mutate(cal_class = ifelse(cal_probability > .50, "1", "0")) %>%
  rename(game_won_.pred_1 = cal_probability, 
         favorite = cal_class) %>%
  mutate(favorite = factor(favorite,levels = c("1","0"))) %>%
  mutate(game_won_.pred_0 = 1 - game_won_.pred_1)

### ---# Step 3: Predict on Unplayed Games Using the Fully Fitted Model----
# Since final_model is a fully fitted workflow, you can call predict() directly on new data.
unplayed_predictions <- predict(final_model, new_data = unplayed_games, type = "prob")
unplayed_class <- predict(final_model, new_data = unplayed_games)
unplayed_cal_preds <- unplayed_predictions  %>% cal_apply(cv_cal_mod)
stopCluster(cl)

# Combine predictions with unplayed games for further use (e.g., logging, analysis)
unplayed_results <- unplayed_games %>%
  mutate(pred_probability = unplayed_predictions$.pred_1,
         pred_class = unplayed_class$.pred_class,
         cal_probability = unplayed_cal_preds$.pred_1,
         cal_class = ifelse(cal_probability > .50, 1, 0),
         prediction_time = Sys.time(),
         model_version = "rf_v_25_with_gamewon",   # define this near the top of your script
         # Initially, actual outcome is unknown
         actual_outcome = NA)

# Save predictions
saveRDS(unplayed_results, file = paste0(rds_files_path, "/Data/team_unplayed_games_rf_predictions_spread.rds"))
saveRDS(unplayed_cal_preds, file = paste0(rds_files_path, "/Data/team_unplayed_games_rf_preds_cal_spread.rds"))
rm(unplayed_class, unplayed_cal_preds, unplayed_predictions,final_model)


#### -----Update Deployment Log------
log_file <- paste0(rds_files_path, "/Data/team_unplayed_predictions_spread_rf.csv")

# Define key columns and prediction columns
key_cols <- c("game_id", "teamId")
pred_cols <- c("pred_probability", "pred_class", "cal_probability")

if (file.exists(log_file)) {
  # Read the existing log
  old_log <- read.csv(log_file, stringsAsFactors = FALSE)
  # Match column types between new and old data (especially pred_class)
  old_log$teamId <- as.character(old_log$teamId)
  unplayed_results$teamId <- as.character(unplayed_results$teamId)
  old_log$pred_class <- as.character(old_log$pred_class)
  unplayed_results$pred_class <- as.character(unplayed_results$pred_class)
  old_log$game_date <- as.Date(old_log$game_date)
  unplayed_results$game_date <- as.Date(unplayed_results$game_date)
  old_log$home_id <- as.character(old_log$home_id)
  unplayed_results$home_id <- as.character(unplayed_results$home_id)
  old_log$away_id <- as.character(old_log$away_id)
  unplayed_results$away_id <- as.character(unplayed_results$away_id)
  old_log$prediction_time <- as_datetime(old_log$prediction_time)
  unplayed_results$prediction_time <- as_datetime(unplayed_results$prediction_time)
  old_log$game_won <- as.factor(old_log$game_won)
  unplayed_results$game_won <- as.factor(unplayed_results$game_won)
  unplayed_results$favorite <- as.factor(unplayed_results$favorite)
  old_log$favorite <- as.factor(old_log$favorite)
  # # Keep rows from new predictions that are different on keys and prediction columns
  # new_to_add <- unplayed_results %>%
  #   # Use a join that compares both keys and prediction columns
  #   anti_join(old_log, by = c(key_cols, pred_cols))
  # 
  # # Append only the new/changed rows to the existing log
  # updated_log <- bind_rows(old_log, new_to_add)
  updated_log <- bind_rows(old_log, unplayed_results)
  # Write the updated log back to file
  write.csv(updated_log, log_file, row.names = FALSE)
} else {
  # If no log exists, write unplayed_results as the new log
  write.csv(unplayed_results, log_file, row.names = FALSE)
}

#---- NEURAL NET VIRTUAL ENVIRONEMNT SETUP----
# Optionally install Python 3.11 if not already installed.
# This installs a version of Python (via Miniconda) that reticulate can use.
# reticulate::install_python(version = "3.11.0")

# Load the reticulate package
library(reticulate)

# # Replace the string below with your actual 3.11 path
# use_python("C:/Users/schne/AppData/Local/r-reticulate/r-reticulate/pyenv/pyenv-win/versions/3.11.0/python.exe", required = TRUE)
# 
# # Verify
# py_config()
# 
# # Verify that the HOME environment variable is set correctly
print(path.expand("~"))  # Expected output: "C:/Users/schne"
# 
# # Clear any interfering environment variables
# Sys.unsetenv("VIRTUAL_ENV")
# Sys.unsetenv("RETICULATE_PYTHON")

# # Optionally, remove these variables from startup files
# renviron_path <- "~/.Renviron"
# if (file.exists(renviron_path)) {
#   renviron_content <- readLines(renviron_path)
#   renviron_content <- renviron_content[!grepl("VIRTUAL_ENV|RETICULATE_PYTHON", renviron_content)]
#   writeLines(renviron_content, renviron_path)
# }
# 
# rprofile_path <- "~/.Rprofile"
# if (file.exists(rprofile_path)) {
#   rprofile_content <- readLines(rprofile_path)
#   rprofile_content <- rprofile_content[!grepl("VIRTUAL_ENV|RETICULATE_PYTHON", rprofile_content)]
#   writeLines(rprofile_content, rprofile_path)
# }
# 
# 
# # Ensure that the .virtualenvs directory exists in your new home directory
# venv_dir <- file.path(path.expand("~"), ".virtualenvs")
# if (!dir.exists(venv_dir)) {
#   dir.create(venv_dir, recursive = TRUE)
# }
# print(venv_dir)  # Should now be "C:/Users/schne/.virtualenvs"
# 
# # Create the virtual environment if it doesn't exist
# if (!virtualenv_exists("r-tensorflow")) {
#   message("Environment does not exist! Creating now...")
#   virtualenv_create("r-tensorflow", python = py_config()$python)
# }
# 
# # Step 6: Install the required Python packages into the virtual environment
# py_install(
#   packages = c("numpy==1.24.3", "tensorflow==2.13.0", "keras==2.13.1"),
#   envname = "r-tensorflow",
#   pip = TRUE
# )

# Step 7: Activate the virtual environment
use_virtualenv("C:/Users/schne/.virtualenvs/r-tensorflow", required = TRUE)

# Step 8: Verify the Python configuration
py_config()

# Step 9: Check if TensorFlow and Keras modules are available and import them
if (py_module_available("tensorflow") && py_module_available("keras")) {
  tf <- import("tensorflow")
  k <- import("keras")
  
  # Check their versions
  cat("TensorFlow Version:", tf$`__version__`, "\n")
  cat("Keras Version:", k$`__version__`, "\n")
} else {
  stop("TensorFlow or Keras is not available in the virtual environment.")
}
## ----NEURAL NET - WIN GAME - PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(tidymodels)
library(doParallel)
library(themis)
library(glmnet)
library(dplyr)
library(purrr)
library(finetune)
library(probably)
library(betacal)
library(themis)
library(keras)
library(tensorflow)

### ----Fit your workflow on all training data for deployment-------------
# This creates a fully fitted model using all of team_df_played
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
final_mlp_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_mlp.rds"))
final_mlp_wf$fit$actions$model$spec

# Custom model-building function
build_custom_mlp <- function(hidden_units, penalty) {
  keras_model_sequential() %>%
    layer_dense(
      units = hidden_units,
      activation = 'relu',
      input_shape = c(hu_s)-1,  # actual number of features - 1 to remove predictor
      kernel_regularizer = regularizer_l2(l2 = penalty)
    ) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = .3) %>% # Dropout layer
    # Second Hidden Layer
    layer_dense(
      units = round(floor(sqrt(hidden_units))),
      activation = 'relu', kernel_regularizer = regularizer_l2(l2 = penalty)) %>%
    layer_batch_normalization() %>%
    layer_dropout(rate = .15) %>%  # Dropout layer
    # Third Hidden Layer
    layer_dense(
      units = round(floor(hidden_units/2)),
      activation = 'relu', kernel_regularizer = regularizer_l2(l2 = penalty)) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = 0.3) %>%  # Dropout layer
    layer_dense(units = 1, activation = 'sigmoid') %>%
    compile(
      optimizer = optimizer_adam(learning_rate = 0.001),
      # optimizer = 'adam',
      loss = 'binary_crossentropy',
      metrics = c('binary_accuracy')
    )
}

# Define Early Stopping callback
# callback_reduce_lr_on_plateau()
early_stop <- callback_early_stopping(
  monitor = "val_loss",
  # monitor = "loss", # Metric to monitor (val_loss not available)
  patience = 15, # Number of epochs with no improvement
  restore_best_weights = TRUE # Restore model weights from the epoch with the best value of the monitored metric
)

reduce_lr <- callback_reduce_lr_on_plateau(
  monitor = "val_loss",
  factor = 0.5,
  patience = 5,
  min_lr = 1e-6,
  verbose = 1
)
gc()

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Fit your workflow on all training data for deployment-------------
# 1. Create a fully fitted model using all of team_df_played
final_model <- fit(final_mlp_wf, data = team_df_played)
train_preds <- predict(final_model, new_data = team_df_played, type = "prob")
train_class <- predict(final_model, new_data = team_df_played) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(team_df_played %>% select(game_won))
cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won,
                                estimate = .pred_1)

team_df_cal_preds <- train_model %>% cal_apply(cv_cal_mod)
saveRDS(team_df_cal_preds, file = paste0(rds_files_path, "/Data/team_deployable_model_preds_mlp.rds"))
rm(train_preds, train_class)

### -----Step 2: Refit final model on correct training window-----
team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3611) %>%
  pull(game_id)

# Filter training rows from team_df_played
team_df_played <- team_df_played %>%
  filter(game_id %in% team_df_train)
#Sanity check
length(unique(team_df_played$game_id))
# Optionally, save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_mlp.rds"))
rm(final_mlp_wf)

### ---# Step 3: Predict on Unplayed Games Using the Fully Fitted Model and----
### ----Preprocess the unplayed games data-----------------
train_preds <- final_model %>% predict(team_df_played, type = "prob")
train_class <- predict(final_model, new_data = team_df_played) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(team_df_played %>% select(game_won))
cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won,
                                estimate = .pred_1)
rm(team_df_played, train_preds, train_class)
#### ----Preprocess Unplayed Games ---- 
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds")) %>% filter(game_status == "unplayed")
unplayed_predictions <- predict(final_model, new_data = unplayed_games, type = "prob")
unplayed_class <- predict(final_model, new_data = unplayed_games)
unplayed_cal_preds <- unplayed_predictions  %>% cal_apply(cv_cal_mod)
stopCluster(cl)

# Combine predictions with unplayed games for further use (e.g., logging, analysis)
unplayed_results <- unplayed_games %>%
  mutate(pred_probability = unplayed_predictions$.pred_1,
         pred_class = unplayed_class$.pred_class,
         cal_probability = unplayed_cal_preds$.pred_1,
         cal_class = ifelse(cal_probability > .50, 1, 0),
         prediction_time = Sys.time(),
         model_version = "nn_v_25",   # define this near the top of your script
         # Initially, actual outcome is unknown
         actual_outcome = NA) %>%
  select(-"game_won")

# Save predictions
saveRDS(unplayed_results, file = paste0(rds_files_path, "/Data/team_unplayed_games_mlp_predictions.rds"))
saveRDS(unplayed_cal_preds, file = paste0(rds_files_path, "/Data/team_unplayed_games_mlp_preds_cal.rds"))
rm(unplayed_class, unplayed_cal_preds, unplayed_predictions,final_model)


#### -----Update Deployment Log------
log_file <- paste0(rds_files_path, "/Data/team_unplayed_mlp_predictions.csv")
unplayed_results <- readRDS(paste0(rds_files_path,"/Data/team_unplayed_games_mlp_predictions.rds"))

# Define key columns and prediction columns
key_cols <- c("game_id", "teamId")
pred_cols <- c("pred_probability", "pred_class", "cal_probability")

if (file.exists(log_file)) {
  # Read the existing log
  old_log <- read.csv(log_file, stringsAsFactors = FALSE)
  # Match column types between new and old data (especially pred_class)
  old_log$teamId <- as.character(old_log$teamId)
  unplayed_results$teamId <- as.character(unplayed_results$teamId)
  old_log$pred_class <- as.character(old_log$pred_class)
  unplayed_results$pred_class <- as.character(unplayed_results$pred_class)
  old_log$game_date <- as.Date(old_log$game_date)
  unplayed_results$game_date <- as.Date(unplayed_results$game_date)
  old_log$home_id <- as.character(old_log$home_id)
  unplayed_results$home_id <- as.character(unplayed_results$home_id)
  old_log$away_id <- as.character(old_log$away_id)
  unplayed_results$away_id <- as.character(unplayed_results$away_id)
  old_log$prediction_time <- as_datetime(old_log$prediction_time)
  unplayed_results$prediction_time <- as_datetime(unplayed_results$prediction_time)
  old_log$game_won_spread <- as.factor(old_log$game_won_spread)
  unplayed_results$game_won_spread <- as.factor(unplayed_results$game_won_spread)
  # # Keep rows from new predictions that are different on keys and prediction columns
  # new_to_add <- unplayed_results %>%
  #   # Use a join that compares both keys and prediction columns
  #   anti_join(old_log, by = c(key_cols, pred_cols))
  # 
  # # Append only the new/changed rows to the existing log
  # updated_log <- bind_rows(old_log, new_to_add)
  updated_log <- bind_rows(old_log, unplayed_results)
  # Write the updated log back to file
  write.csv(updated_log, log_file, row.names = FALSE)
} else {
  # If no log exists, write unplayed_results as the new log
  write.csv(unplayed_results, log_file, row.names = FALSE)
}

## ----NEURAL NET - GAME SPREAD - PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(tidymodels)
library(doParallel)
library(themis)
library(glmnet)
library(dplyr)
library(purrr)
library(finetune)
library(probably)
library(betacal)
library(themis)
library(keras)
library(tensorflow)

### ----Fit your workflow on all training data for deployment-------------
# This creates a fully fitted model using all of team_df_played
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_mlp.rds"))
final_mlp_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_mlp_spread.rds"))
final_mlp_wf$fit$actions$model$spec

# Custom model-building function
build_custom_mlp <- function(hidden_units, penalty) {
  keras_model_sequential() %>%
    layer_dense(
      units = hidden_units,
      activation = 'relu',
      input_shape = c(hu_s)-1,  # actual number of features - 1 to remove predictor
      kernel_regularizer = regularizer_l2(l2 = penalty)
    ) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = .3) %>% # Dropout layer
    # Second Hidden Layer
    layer_dense(
      units = round(floor(sqrt(hidden_units))),
      activation = 'relu', kernel_regularizer = regularizer_l2(l2 = penalty)) %>%
    layer_batch_normalization() %>%
    layer_dropout(rate = .15) %>%  # Dropout layer
    # Third Hidden Layer
    layer_dense(
      units = round(floor(hidden_units/2)),
      activation = 'relu', kernel_regularizer = regularizer_l2(l2 = penalty)) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = 0.3) %>%  # Dropout layer
    layer_dense(units = 1, activation = 'sigmoid') %>%
    compile(
      optimizer = optimizer_adam(learning_rate = 0.001),
      # optimizer = 'adam',
      loss = 'binary_crossentropy',
      metrics = c('binary_accuracy')
    )
}

# Define Early Stopping callback
# callback_reduce_lr_on_plateau()
early_stop <- callback_early_stopping(
  monitor = "val_loss",
  # monitor = "loss", # Metric to monitor (val_loss not available)
  patience = 10, # Number of epochs with no improvement
  restore_best_weights = TRUE # Restore model weights from the epoch with the best value of the monitored metric
)

reduce_lr <- callback_reduce_lr_on_plateau(
  monitor = "val_loss",
  factor = 0.5,
  patience = 5,
  min_lr = 1e-6,
  verbose = 1
)
gc()

team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3611) %>%
  pull(game_id)

# Filter training rows from team_df_played
train_df <- team_df_played %>%
  filter(game_id %in% team_df_train)
#Sanity check
length(unique(train_df$game_id))

set.seed(123)
num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

final_model <- fit(final_mlp_wf, data = train_df)
# Optionally, save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_mlp_spread.rds"))

train_preds <- predict(final_model, new_data = train_df, type = "prob")
train_class <- predict(final_model, new_data = train_df) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(train_df %>% select(game_won_spread))

cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won_spread,
                                estimate = .pred_1)

rm(team_df_played, train_preds, train_class)
### -----Step 2: Preprocess the unplayed games data-----------------
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds")) %>% filter(game_status == "unplayed")
unplayed_won_preds <- readRDS(paste0(rds_files_path, "/Data/team_unplayed_games_mlp_predictions.rds"))
# unplayed_won_cal <- readRDS(paste0(rds_files_path,  "/Data/team_unplayed_games_preds_cal_v2.rds"))

unplayed_games <- unplayed_games  %>% 
  left_join(unplayed_won_preds[,c("game_id","teamId","cal_probability")], 
            by = c("game_id", "teamId")) %>%
  mutate(cal_class = ifelse(cal_probability > .50, "1", "0")) %>%
  rename(game_won_.pred_1 = cal_probability, 
         favorite = cal_class) %>%
  mutate(favorite = factor(favorite,levels = c("1","0"))) %>%
  mutate(game_won_.pred_0 = 1 - game_won_.pred_1)

### ---# Step 3: Predict on Unplayed Games Using the Fully Fitted Model----
# Since final_model is a fully fitted workflow, you can call predict() directly on new data.
unplayed_predictions <- predict(final_model, new_data = unplayed_games, type = "prob")
unplayed_class <- predict(final_model, new_data = unplayed_games)
unplayed_cal_preds <- unplayed_predictions  %>% cal_apply(cv_cal_mod)
stopCluster(cl)

# Combine predictions with unplayed games for further use (e.g., logging, analysis)
unplayed_results <- unplayed_games %>%
  mutate(pred_probability = unplayed_predictions$.pred_1,
         pred_class = unplayed_class$.pred_class,
         cal_probability = unplayed_cal_preds$.pred_1,
         cal_class = ifelse(cal_probability > .50, 1, 0),
         prediction_time = Sys.time(),
         model_version = "nn_v_250_with_gamewon",   # define this near the top of your script
         # Initially, actual outcome is unknown
         actual_outcome = NA) %>%
  select(-"game_won_spread")

# Save predictions
saveRDS(unplayed_results, file = paste0(rds_files_path, "/Data/team_unplayed_games_mlp_predictions_spread.rds"))
saveRDS(unplayed_cal_preds, file = paste0(rds_files_path, "/Data/team_unplayed_games_mlp_preds_cal_spread.rds"))
rm(unplayed_class, unplayed_cal_preds, unplayed_predictions,final_model)


#### -----Update Deployment Log------
log_file <- paste0(rds_files_path, "/Data/team_unplayed_predictions_spread_mlp.csv")
unplayed_results <- readRDS(paste0(rds_files_path,"/Data/team_unplayed_games_mlp_predictions_spread.rds"))

# Define key columns and prediction columns
key_cols <- c("game_id", "teamId")
pred_cols <- c("pred_probability", "pred_class", "cal_probability")

if (file.exists(log_file)) {
  # Read the existing log
  old_log <- read.csv(log_file, stringsAsFactors = FALSE)
  # Match column types between new and old data (especially pred_class)
  old_log$teamId <- as.character(old_log$teamId)
  unplayed_results$teamId <- as.character(unplayed_results$teamId)
  old_log$pred_class <- as.character(old_log$pred_class)
  unplayed_results$pred_class <- as.character(unplayed_results$pred_class)
  old_log$game_date <- as.Date(old_log$game_date)
  unplayed_results$game_date <- as.Date(unplayed_results$game_date)
  old_log$home_id <- as.character(old_log$home_id)
  unplayed_results$home_id <- as.character(unplayed_results$home_id)
  old_log$away_id <- as.character(old_log$away_id)
  unplayed_results$away_id <- as.character(unplayed_results$away_id)
  old_log$prediction_time <- as_datetime(old_log$prediction_time)
  unplayed_results$prediction_time <- as_datetime(unplayed_results$prediction_time)
  old_log$game_won <- as.factor(old_log$game_won)
  unplayed_results$game_won <- as.factor(unplayed_results$game_won)
  unplayed_results$favorite <- as.factor(unplayed_results$favorite)
  old_log$favorite <- as.factor(old_log$favorite)
  # # Keep rows from new predictions that are different on keys and prediction columns
  # new_to_add <- unplayed_results %>%
  #   # Use a join that compares both keys and prediction columns
  #   anti_join(old_log, by = c(key_cols, pred_cols))
  # 
  # # Append only the new/changed rows to the existing log
  # updated_log <- bind_rows(old_log, new_to_add)
  updated_log <- bind_rows(old_log, unplayed_results)
  # Write the updated log back to file
  write.csv(updated_log, log_file, row.names = FALSE)
} else {
  # If no log exists, write unplayed_results as the new log
  write.csv(unplayed_results, log_file, row.names = FALSE)
}