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
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_spread.rds"))
team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits_v2.rds"))
rec_bake <- team_recipe %>% prep() %>% bake(., new_data =  NULL)
colnames(rec_bake)
gc()

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
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v2.rds"))
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
cv_preds <- collect_predictions(team_fit)
cv_preds_clean <- cv_preds %>%
  filter(
    is.finite(.pred_1),      # remove NA/NaN/Inf
    .pred_1 >= 0,            # remove negative
    .pred_1 <= 1            # remove >1
  )
rm(cv_preds)

# Set sample size, ensuring you don't exceed available rows
sample_size <-  round(nrow(cv_preds_clean) * 0.10)
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

final_cal_preds %>%
  cal_plot_windowed(truth = game_won_spread, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

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
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
team_wf_log <- readRDS(paste0(rds_files_path,"/Data/team_wf_log_spread.rds"))

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Fit your workflow on all training data for deployment-------------
# This creates a fully fitted model using all of team_df_played
final_model <- fit(team_wf_log, data = team_df_played)
# Optionally, save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_spread.rds"))
rm(team_wf_log)

train_preds <- predict(final_model, new_data = team_df_played, type = "prob")
train_class <- predict(final_model, new_data = team_df_played) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(team_df_played %>% select(game_won_spread))

cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won_spread,
                                estimate = .pred_1)

rm(team_df_played, train_preds, train_class)
### -----Step 2: Preprocess the unplayed games data-----------------
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds")) %>% filter(game_status == "unplayed")

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
         prediction_time = Sys.time(),
         model_version = "glm_v_25",   # define this near the top of your script
         # Initially, actual outcome is unknown
         actual_outcome = NA) %>%
  select(-"game_won_spread")

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
