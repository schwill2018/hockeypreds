library(ranger)
library(tidyverse)
library(tidymodels)
library(finetune)
library(iml)
library(gridExtra)
library(future)
library(future.callr)
library(tictoc)
library(doParallel)
# library(foreach)

rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_goal_v3.rds"))
team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits_v3.rds"))
impt_df <- readRDS(paste0(rds_files_path, "/Data/team_fimp_tail.rds"))
shap_knee <- readRDS(paste0(rds_files_path, "/Data/team_shap_knee_drop.rds"))
pareto_drop <- readRDS(paste0(rds_files_path, "/Data/team_shap_pareto_drop.rds"))
gc()

# Update Recipe after Examining VI, ALE, and SHAP plots
# to_remove <- tail(impt_df$feature, 137) %>% str_subset("season", negate = TRUE)
to_remove <- shap_knee %>%
  str_subset("season", negate = TRUE) %>%
  str_subset("is_home", negate = TRUE) %>%
  str_subset("gameType", negate = TRUE) %>%
  str_subset("is_back_to_back", negate = TRUE) %>%
  str_subset("b2b_win_ratio_lag", negate = TRUE) %>%
  str_subset("no_feats_games", negate = TRUE)

patterns <- c("rolling_home_distance","rolling_away_distance","tz_diff_game",
              "home_venue_time_diff") #feature removals discovered
pattern_regex <- paste(patterns, collapse = "|")
cols_to_drop   <- grep(pattern_regex, names(team_df_played), value = TRUE)

team_recipe <- recipe(game_won ~ ., data = team_df_played) %>%
  step_rm(game_status) %>% #step_filter(gameType == 2) %>%
  step_rm(game_won_spread) %>%
  step_rm(playerId) %>%
  step_rm(prior_rank, rolling_rank, rolling_points, cum_points, cum_rank) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id","venue_time", "game_time"))) %>%
  step_rm(any_of(!!cols_to_drop)) %>%
  # step_rm(any_of(!!to_remove)) %>%
  step_rm(last_period_type) %>%
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, opp_teamId, new_role = "ID")%>%
  update_role(game_date, new_role = "DATE") %>% 
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  step_mutate(is_home = as.factor(is_home)) %>%
  step_mutate(season = as.factor(season)) %>%
  step_mutate(gameType = as.factor(gameType)) %>%
  step_mutate(is_back_to_back = as.factor(is_back_to_back)) %>%
  step_mutate(no_feats_games = as.factor(no_feats_games)) %>%
  step_mutate(elo_class = as.factor(elo_class)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel(all_nominal_predictors(), -is_home, -gameType, -is_back_to_back,
             -no_feats_games,elo_class) %>%
  step_dummy(all_nominal_predictors()) 
rec_bake <- team_recipe %>% prep() %>% bake(new_data = NULL)
colnames(rec_bake)
gc()

# ### ----ROLLING CV (250 GAME SPLIT)-----
# # Create a game-level data frame
# game_level_df <- team_df_played %>%
#   distinct(game_id, game_date, startTimeUTC) %>%
#   arrange(startTimeUTC, game_id) %>%
#   mutate(game_index = row_number())
# 
# # Create rolling origin resamples at the game level
# game_splits <- rolling_origin(
#   data = game_level_df,
#   initial = 3611,   # Approx. _ season
#   assess = 250,      # Approx. _ games in the test set
#   cumulative = FALSE,
#   skip = 250       # No overlap between test sets
# )
# 
# # Translate splits to player level
# translate_splits <- function(spl) {
#   train_games <- analysis(spl)$game_id
#   test_games <- assessment(spl)$game_id
#   
#   # Ensure same-date games are handled correctly
#   train_dates <- game_level_df %>%
#     filter(game_id %in% train_games) %>%
#     pull(startTimeUTC)
#   
#   test_dates <- game_level_df %>%
#     filter(game_id %in% test_games) %>%
#     pull(startTimeUTC)
#   
#   #find player rows that match those training or test dates
#   train_indices <- which(team_df_played$startTimeUTC %in% train_dates)
#   test_indices <- which(team_df_played$startTimeUTC %in% test_dates)
#   
#   rsample::make_splits(
#     list(analysis = train_indices, assessment = test_indices),
#     data = team_df_played
#   )
# }
# 
# # Set up parallel backend
# num_cores <- detectCores()
# cl <- makeCluster(max(0,num_cores-2))
# registerDoParallel(cl)
# 
# # Step 1: Translate all game-level splits into player-level splits
# team_splits_list <- map(game_splits$splits, translate_splits)
# 
# # Step 4: Create the rset object for the remaining splits
# team_splits <- rsample::manual_rset(
#   splits = team_splits_list,
#   ids = game_splits$id
# )
# final_split <- team_splits$splits[[dim(team_splits)[1]]]
# team_splits <- team_splits[-dim(team_splits)[1],]
# 
# # Ensure team_splits is a valid rset object
# class(team_splits) <- c("manual_rset", "rset", "tbl_df", "tbl", "data.frame")
# rm(team_splits_list)
# 
# for (s in 1:dim(team_splits)[1]) {
#   #Check logic working
#   first_split <- team_splits$splits[[s]]
#   train_indices <- analysis(first_split)
#   test_indices <- assessment(first_split)
#   
#   print(paste0("NA's in ",s,"train split: ",sum(colSums(is.na(train_indices)))))
#   print(paste0("NA's in ",s,"test split: ",sum(colSums(is.na(test_indices)))))
#   
# }
# 
# rm(train_indices)
# rm(test_indices)
# rm(first_split)
# 

# ---------------------------
# Define Model
# ---------------------------
set.seed(123)
#Define Cluster for Parallel Proccessing
num_cores <- detectCores()-4
cl <- makeCluster(max(1, num_cores))
registerDoParallel(cl)

# num_splits <-dim(team_splits)[1]
# para_threads <- floor(num_cores/num_splits)
# print(para_threads)

# Random Forest for Rolling CV
team_rf_rolling <- rand_forest(mtry = tune(), min_n = tune(), trees = tune())%>%
  set_mode("classification") %>%
  set_engine("ranger",
             importance  = "permutation")


# ---------------------------
# Create Workflow
# ---------------------------
# team_recipe2 <- recipe(game_won ~ ., data = team_df_played) %>%
#   step_rm(game_status) %>%
#   # step_rm(team_game_spread) %>%
#   step_rm(game_won_spread) %>%
#   step_rm(playerId) %>%
#   step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
#                    "away_team_locale","home_team_name", "home_team_locale", 
#                    "winning_team","winning_team_id"))) %>% #variable performance test recipe
#   step_rm(matches("7")) %>%
#   # Assign specific roles to ID columns
#   update_role(game_id, home_id, away_id, teamId, opp_teamId,
#               new_role = "ID") %>%
#   update_role(game_date, new_role = "DATE") %>%
#   update_role(startTimeUTC, new_role = "DATETIME") %>%
#   step_mutate(is_home = as.factor(is_home)) %>%
#   step_mutate(season = as.factor(season)) %>%
#   step_zv() %>%
#   step_normalize(all_numeric_predictors()) %>%
#   step_novel(all_nominal_predictors(), -is_home) %>%
#   step_dummy(all_nominal_predictors()) 

team_wf_rf_rolling <- workflow() %>%
  add_recipe(team_recipe) %>%
  add_model(team_rf_rolling)
rec_bake <- team_recipe %>% prep() %>% bake(., new_data =  NULL)

# # ---------------------------
# # Define Hyperparameter Grids
# # ---------------------------
# # Grid for Random Forest
# 
upper_g <- ceiling(sqrt(length(colnames(team_df_played))))
upper_min_n <- upper_g - 2
max_feats <- (length(rec_bake) - dim(team_recipe$var_info %>%
                                       filter(role != "predictor") %>%
                                       select(variable) %>%
                                       unique())[1]) %>% sqrt() %>% ceiling()
min_feats <- (length(rec_bake) - dim(team_recipe$var_info %>%
                                       filter(role != "predictor") %>%
                                       select(variable) %>%
                                       unique())[1]) %>% log2() %>% floor() +1
max_min_n <- ceiling(sqrt(3611))
min_min_n <- 5
rf_grid <- grid_regular(
  mtry(range = c(min_feats,max_feats)), #c(3,23)
  min_n(range = c(min_min_n,max_min_n)), #c(5,15)
  levels = 3)

rf_params <- parameters(team_wf_rf_rolling) %>%
  update(
    mtry    = mtry(range = c(min_feats, max_feats)),
    min_n   = min_n(range = c(min_min_n, max_min_n)),
    trees   = trees(range = c(75, 575))       # adjust range to taste
  )
rf_params

set.seed(123)                    # reproducible
rf_grid_lh <- grid_space_filling(
  rf_params,
  type = "latin_hypercube",
  size = 7                 # number of Latin-hypercube points
)

# rf_ctrl <- control_bayes(
#   verbose = TRUE,
#   save_pred = TRUE,
#   allow_par = TRUE,
#   parallel_over = "resamples",
#   no_improve = 2           # stop if no improvement in 5 consecutive iterations
# )
# set.seed(123)
# rf_res_rolling_bayes <- tune_bayes(
#   object     = team_wf_rf_rolling,
#   resamples  = team_splits,
#   param_info = rf_params,
#   initial    = 5,            # 10 random initial points
#   iter       = 3,            # up to 20 Bayesian iterations
#   metrics    = metric_set(accuracy,kap,roc_auc,brier_class,
#                           yardstick::spec,
#                           yardstick::sens),
#   control = rf_ctrl)
# stopCluster(cl)
rm(team_df_played)

# # ---------------------------
# # Define Grid Control
# # ---------------------------
control_settings <- control_grid(
  verbose = TRUE,   # prints progress
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples"
)

# # ---------------------------
# # Fit Resamples
# # ---------------------------
# # Tuning sets for RF on rolling CV
tic()
rf_res_rolling <- tune_grid(
  team_wf_rf_rolling, 
  resamples = team_splits,
  grid = rf_grid_lh,
  metrics = metric_set(accuracy, 
                       kap,
                       roc_auc,
                       brier_class,
                       yardstick::spec,
                       yardstick::sens),
  control = control_settings
)
toc()
stopCluster(cl)
# ---------------------------
#  Save Random Forest Tuning Results
# ---------------------------
saveRDS(rf_res_rolling, file = paste0(rds_files_path, "/Data/team_rocv_res_rf_fit_v3.rds"))
saveRDS(team_wf_rf_rolling, file = paste0(rds_files_path, "/Data/team_wf_rf_v3.rds"))

rf_res_rolling <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_rf_fit_v3.rds"))
team_wf_rf_rolling <- readRDS(paste0(rds_files_path, "/Data/team_wf_rf_v3.rds"))
gc()

team_metrics <- rf_res_rolling %>% collect_metrics(summarize = FALSE) %>%
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

# ---------------------------
# Select Best Hyperparameters
# ---------------------------
best_rf_rolling <- select_best(rf_res_rolling, metric = "roc_auc")

best_mtry <- best_rf_rolling %>% pull(mtry)
best_min_n <- best_rf_rolling %>% pull(min_n)
team_metrics <- rf_res_rolling %>% collect_metrics(summarize = FALSE)
team_metrics_best <- team_metrics %>% 
  filter(mtry == best_mtry, min_n == best_min_n)
team_metrics_avg <- team_metrics_best %>%
  group_by(.metric) %>%
  summarize(mean_est = mean(.estimate, na.rm = TRUE), .groups = "drop")
team_metrics_avg 

# Plot the per-split performance for the best model
ggplot(team_metrics_best, aes(x = id, y = .estimate, color = .metric, group = .metric)) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  labs(
    title = "ROC AUC Performance for Best Hyperparameters Across Rolling Splits",
    x = "Rolling Splits",
    y = "ROC AUC Estimate"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
# ---------------------------
# 10. Finalize the Workflow
# ---------------------------
final_rf_wf <- finalize_workflow(team_wf_rf_rolling, best_rf_rolling)

# ---------------------------
# Final Model Evaluation with last_fit()
# ---------------------------
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens)
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))

final_rf_fit <- final_rf_wf %>%
  last_fit(final_split, 
           metrics = final_metrics_set)

# ---------------------------
#  Collect and Save Final Metrics and Predictions
# ---------------------------
final_rf_metrics <- collect_metrics(final_rf_fit)
print(final_rf_metrics)

final_rf_predictions <- collect_predictions(final_rf_fit)

saveRDS(final_rf_wf, file = paste0(rds_files_path, "/Data/team_final_wf_rf_v3.rds"))
saveRDS(final_rf_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_fit_v3.rds"))
saveRDS(final_rf_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_metrics_v3.rds"))
saveRDS(final_rf_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_predictions_v3.rds"))

rm(final_rf_fit)
rm(final_rf_metrics)
rm(final_rf_predictions)
rm(rf_res_rolling)
# ---------------------------
# Stop the Parallel Cluster
# ---------------------------
stopCluster(cl)
gc()

## ----MODEL CALIBRATION (RANDOM FOREST)----------------------------------------------------------------------------------------------------------------------------------
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
library(ranger)

rds_files_path <- getwd()
team_wf_rf<- readRDS(paste0(rds_files_path,"/Data/team_wf_rf.rds"))
team_fit <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_rf_fit_v3.rds"))
final_predictions <- readRDS(paste0(rds_files_path, "/Data/team_rocv_final_rf_predictions_v3.rds"))
final_rf_metrics <- readRDS(paste0(rds_files_path, "/Data/team_rocv_final_rf_metrics_v3.rds"))
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
rf_facet <- cv_preds_sample %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Game Won (Random Forest, Resamples)")
rf_facet

# Resample
cal_plot_breaks(cv_preds_sample, truth = game_won,
                estimate = .pred_1, event_level = "first")
# Test
cal_plot_breaks(final_predictions, truth = game_won, 
                estimate = .pred_1, event_level = "first")


### ------Calibrate the Model on the training data---------------------
cv_preds_clean <- cv_preds_clean %>%
  filter(!is.na(.pred_1), !is.na(game_won)) #%>%
# mutate(.pred_1 = pmin(pmax(.pred_1, 0.00001), 0.99999))

cv_cal_mod <- cal_estimate_beta(cv_preds_clean, truth = game_won)
train_beta_cal <- cv_preds_clean %>% cal_apply(cv_cal_mod)
cls_met <- metric_set(roc_auc, brier_class)
oth_met <- metric_set(yardstick::specificity, yardstick::sensitivity)
train_beta_cal %>% cls_met(game_won, .pred_1)
# train_beta_cal %>% oth_met(game_won,.pred_0)

train_beta_cal_samp <- train_beta_cal %>% 
  slice_sample(n = min(n_rows, sample_size))
train_beta_cal_samp %>%
  cal_plot_windowed(truth = game_won, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

rf_train_facet <- train_beta_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Calibrated Probability Estimate of Game Won (Random Forest, Resamples)")
rf_train_facet

rm(team_fit)
##
# -----Predict on the Final Holdout Split------------------
# Apply the calibration model to final predictions
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v2.rds"))

final_cal_preds <- final_predictions %>% cal_apply(cv_cal_mod)
final_fit <- assessment(final_split) %>% 
  rename(game_won_actual = game_won) %>%
  bind_cols(final_cal_preds)

final_metrics_1 <- final_fit %>% cls_met(game_won_actual, .pred_1)
print(final_metrics_1)

final_cal_samp <- final_cal_preds %>% 
  slice_sample(n = min(n_rows, sample_size))
final_cal_samp %>%
  cal_plot_windowed(truth = game_won, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

rf_test_facet <- final_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Calibrated Probability Estimate of Point Earned (Random Forest, Test)")
rf_test_facet

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_fit_cal.rds"))
saveRDS(final_metrics_1, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_metrics_cal.rds"))
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
library(ranger)

# Assume you already have these objects saved from your prior work:
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
final_rf_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_rf.rds"))

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Refit final model on correct training window-----
team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  dplyr::slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3611) %>%
  pull(game_id)

# Filter training rows from team_df_played
team_df_played <- team_df_played %>%
  filter(game_id %in% team_df_train)
#Sanity check
length(unique(team_df_played$game_id))

final_model <- fit(final_xgb_wf, data = team_df_played)
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

# save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_rf.rds"))
rm(final_xgb_wf)

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
