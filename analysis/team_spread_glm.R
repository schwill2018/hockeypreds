## -LOGISTIC REGRESSION-------------------------------------------------------------------------------------------------------------------------------------
library(tidymodels)
library(zoo)
library(tidyverse)
library(glmnet)
library(tictoc)
library(finetune)
library(naniar)
library(doParallel)
library(dplyr)
library(purrr)
library(themis)
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

rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
all_boxscore_df <- readRDS(paste0(rds_files_path, "/Data/combined_2009_2024_boxscore_v2.rds"))
game_scores <- all_boxscore_df %>% distinct(game_id, teamId, away_score, home_score, season) %>%
  filter(season >= 2019) %>% select(-season)
team_df_played <- team_df_played %>% inner_join(game_scores, by = c("game_id","teamId"))
rm(all_boxscore_df,game_scores)

# team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits_v2.rds"))
# team_fit <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_glm_fit_v2.rds"))
# team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_spread.rds"))


game_won_preds <- readRDS(paste0(rds_files_path, "/Data/team_deployable_model_preds_glm.rds"))
team_df_played <- team_df_played %>% 
  bind_cols(game_won_preds[,c(".pred_1",".pred_0",".pred_class")]) %>%
  rename(game_won_.pred_1 = .pred_1, 
         game_won_.pred_0 = .pred_0,
         favorite = .pred_class) %>%
  mutate(game_won_spread = case_when(
    favorite == 1 & abs(home_score - away_score) >= 2 ~ "1",   # Favorite covers if wins by 2 or more
    favorite == 0 & abs(home_score - away_score) <= 1 ~ "1",   # Underdog covers if loses by 1 or wins
    TRUE ~ "0")) %>%
  mutate(favorite = factor(favorite, levels = c("1", "0"))) %>%
  mutate(game_won_spread = factor(game_won_spread, levels = c("1", "0"))) %>%
  select(-home_score, -away_score)
saveRDS(team_df_played, file = paste0(rds_files_path, "/Data/team_df_played_glm.rds"))

team_recipe <- recipe(game_won_spread ~ ., data = team_df_played) %>%
  step_rm(game_status) %>%
  # step_rm(team_game_spread) %>%
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
  step_mutate(favorite = as.factor(favorite)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel(all_nominal_predictors(), -is_home, -favorite) %>%
  step_dummy(all_nominal_predictors())

rec_bake <- team_recipe %>% prep() %>% bake(., new_data =  NULL)
colnames(rec_bake)
rm(rec_bake)
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

saveRDS(final_split, file = paste0(rds_files_path, "/Data/team_final_split_spread.rds"))
saveRDS(team_splits, file = paste0(rds_files_path, "/Data/team_splits_spread.rds"))

stopCluster(cl)

# Set up parallel backend
num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

team_log <- logistic_reg() %>% 
  set_mode("classification") %>% 
  set_engine("glm")

team_wf_log <- workflow() %>% 
  add_recipe(team_recipe) %>% 
  add_model(team_log)


###-----Define Resample Control-----

control_settings <- control_resamples(
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples" #ONLY CHOOSE EVERYTHING IF COMPUTATION RESOURCES AVAILABLE!!!!
)

start_t <- Sys.time()
#Now, fit_resamples using the player-level rolling splits
team_fit <- fit_resamples(
  team_wf_log,
  resamples = team_splits,  
  metrics = metric_set(accuracy, kap, roc_auc, brier_class,yardstick::spec, yardstick::sens),
  control = control_settings)

end_t <- Sys.time()
end_t - start_t

# # Extract coefficients after resampling
# res_fit_model <- team_fit %>% extract_fit_parsnip()
# # Get the coefficients for the logistic regression model
# res_coefficients <- tidy(res_fit_model)
# # View coefficients
# print(res_coefficients)

team_fit %>% collect_metrics()

#Save Memory
rm(team_splits)
gc()

# Collect metrics from rolling CV
saveRDS(team_wf_log, file = paste0(rds_files_path, "/Data/team_wf_log_spread.rds"))
saveRDS(team_fit, file = paste0(rds_files_path, "/Data/team_rocv_res_glm_fit_spread.rds"))

library(ggplot2)

# Extract and organize metrics
team_metrics <- team_fit %>% collect_metrics(summarize = FALSE) %>%
  filter(.metric != "kap") %>%
  filter(.metric != "brier_class")

# Plotting metrics over rolling splits
ggplot(team_metrics, aes(x = id, y = .estimate, color = .metric, group = .metric)) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  labs(
    title = "Model Metrics Over Rolling Splits",
    x = "Rolling Splits",
    y = "Metric Estimate",
    color = "Metric"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

#Save Memory
rm(team_fit, team_recipe)
gc()

###----Final Model Evaluation with last_fit()----
# Define metric set for final evaluation
# Extract the Last Split for Final Evaluation
# final_train <- analysis(final_split)
# final_test <- assessment(final_split)

registerDoParallel(cl)
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_spread.rds"))
final_metrics_set <- metric_set(
  accuracy, 
  kap, 
  roc_auc, 
  brier_class, 
  yardstick::spec, 
  yardstick::sens)

# Perform final fit and evaluation
final_fit <- team_wf_log %>%
  last_fit(final_split, 
           metrics = final_metrics_set)

###----Collect and Save Final Metrics and Predictions----
# Collect final metrics
final_metrics <- final_fit %>% collect_metrics()
print(final_metrics)

# Collect final predictions
final_predictions <- final_fit %>% collect_predictions()
stopCluster(cl)

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_fit_spread.rds"))
saveRDS(final_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_metrics_spread.rds"))
saveRDS(final_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_predictions_spread.rds"))


###-----Inspect Final Metrics and Predictions-----
# View final metrics
print(final_metrics)
head(final_predictions) 
gc()

# Extract coefficients after resampling
final_fit_model <- final_fit %>% extract_fit_parsnip()
# Get the coefficients for the logistic regression model
final_coefficients <- tidy(final_fit_model)
# View coefficients
print(final_coefficients)

## ----MODEL CALIBRATION (LOGISTIC REGRESSION)----------------------------------------------------------------------------------------------------------------------------------
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
library(probably)
library(betacal)
library(themis)

rds_files_path <- getwd()
team_wf_log<- readRDS(paste0(rds_files_path,"/Data/team_wf_log_spread.rds"))
team_fit <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_glm_fit_spread.rds"))
final_predictions <- readRDS(paste0(rds_files_path, "/Data/team_rocv_final_glm_predictions_spread.rds"))
gc()

# Define metric set once
final_metrics_set <- metric_set(
  accuracy, 
  kap, 
  roc_auc, 
  brier_class, 
  yardstick::spec, 
  yardstick::sens
)


### ----------Visualize Calibration (Distribution of Predictions)-----------------
#Collect all out-of-sample predictions fit_resamples() result
set.seed(123)
best_params <- select_best(team_fit, metric = "roc_auc")
print(best_params)
cv_preds <- team_fit %>% collect_predictions() %>%
  filter(.config == best_params$.config)
cv_preds_clean <- cv_preds %>%
  filter(
    is.finite(.pred_1),      # remove NA/NaN/Inf
    .pred_1 >= 0,            # remove negative
    .pred_1 <= 1            # remove >1
  )
rm(cv_preds)

# Set sample size, ensuring you don't exceed available rows
sample_size <-  round(nrow(cv_preds_clean))
n_rows <- nrow(cv_preds_clean)
cv_preds_sample <- cv_preds_clean %>% 
  slice_sample(n = min(n_rows, sample_size))

# Plot using the sampled data
log_facet <- cv_preds_sample %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won_spread, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
log_facet

# Resample
cal_plot_breaks(cv_preds_sample, truth = game_won_spread, 
                estimate = .pred_1, event_level = "first")

### ------Calibrate the Model on the training data---------------------
cv_cal_mod <- cal_estimate_beta(cv_preds_clean, truth = game_won_spread,
                                estimate = .pred_1)
train_beta_cal <- cv_preds_clean %>% cal_apply(cv_cal_mod)
cls_met <- metric_set(roc_auc, brier_class)
oth_met <- metric_set(yardstick::specificity, yardstick::sensitivity)
train_beta_cal %>% cls_met(game_won_spread, .pred_1)

train_beta_cal_samp <- train_beta_cal %>% 
  slice_sample(n = min(n_rows, sample_size))
train_beta_cal %>%
  cal_plot_windowed(truth = game_won_spread, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

log_train_facet <- train_beta_cal %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won_spread, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
log_train_facet

rm(team_fit)
### -----Predict on the Final Holdout Split------------------
# Apply the calibration model to final predictions
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v2.rds"))

final_cal_preds <- final_predictions %>% cal_apply(cv_cal_mod)
final_fit <- assessment(final_split) %>% 
  rename(earned_team_actual = game_won_spread) %>%
  bind_cols(final_cal_preds)

final_metrics_1 <- final_fit %>% cls_met(earned_team_actual, .pred_1)
print(final_metrics_1)
# final_metrics_2 <- final_fit %>% oth_met(game_won_spread, .pred_1)
# print(final_metrics_2)

final_cal_samp <- final_cal_preds %>% 
  slice_sample(n = min(n_rows, sample_size))

# Test
cal_plot_breaks(final_cal_preds, truth = game_won_spread, 
                estimate = .pred_1, event_level = "first")

#Window only makes sense with larger holdout sets
# final_cal_preds %>%
#   cal_plot_windowed(truth = game_won_spread, estimate = .pred_1, 
#                     event_level = "first", step_size = 0.025)

log_test_facet <- final_cal_preds %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won_spread, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
log_test_facet

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_fit_cal_spread.rds"))
saveRDS(final_metrics_1, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_metrics_cal_spread.rds"))
rm(team_wf_log)
gc()

## ----PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
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
