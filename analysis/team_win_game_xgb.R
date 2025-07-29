library(tidymodels)
library(themis)
library(stringr)   
library(xgboost)      
library(dplyr)        
library(parsnip)      
library(workflows)    
library(rsample)      
library(tune)         
library(doParallel)
library(dials)
library(tictoc)
library(probably)
library(betacal)

rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_goal_v3.rds"))
team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits_v4.rds"))
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

# ---------------------------
# XGBoost Workflow (structurally mirroring your RF code)
# ---------------------------
set.seed(123)
#--- 1) Parallel backend (same as RF) ---
num_cores <- detectCores() - 3
cl <- makeCluster(max(1, num_cores))
registerDoParallel(cl)

#--- 2) Reuse the same recipe (team_recipe2) ---
#    (Assumes team_recipe2 is already defined exactly as in your RF workflow)
#    If not, copy‐paste your team_recipe2 definition here.

#--- 3) Compute mtry / min_n ranges based on rec_bake (same logic as RF) ---
#    Count total columns after baking, minus any non-predictors
num_total_cols <- ncol(rec_bake)
num_non_pred_vars <- team_recipe$var_info %>%
  filter(role != "predictor") %>%
  pull(variable) %>%
  unique() %>%
  length()

num_predictors <- num_total_cols - num_non_pred_vars

max_feats  <- ceiling(sqrt(num_predictors))
min_feats  <- floor(log2(num_predictors)) + 1
max_min_n  <- ceiling(sqrt(3611))   # same as RF
min_min_n  <- 5                     # same as RF


#--- 4) Define XGBoost model spec with tunable parameters ---
xgb_spec <- boost_tree(
  trees         = tune(),
  tree_depth    = tune(),
  learn_rate    = tune(),
  loss_reduction = tune(),
  sample_size   = tune(),
  mtry          = tune(),
  min_n         = tune()) %>%
  set_engine("xgboost", objective = "binary:logistic", eval_metric = "auc") %>%
  set_mode("classification")

#--- 5) Workflow (recipe + model) ---
team_wf_xgb <- workflow() %>%
  add_recipe(team_recipe) %>%
  add_model(xgb_spec)

#--- 6) Define a random grid for XGBoost tuning ---
#    We draw 30 random combinations to keep the grid manageable.
xgb_grid <- grid_space_filling(type = "latin_hypercube", 
                               trees(range = c(150,1500)),
                               tree_depth(range = c(1, 15)),
                               learn_rate(range = c(-3, -0.5),
                                          trans = log10_trans()),
                               loss_reduction(range = c(-6, 1), 
                                              trans = log10_trans()),
                               sample_prop(range = c(0.5,1)),
                               mtry(range = c(min_feats, max_feats)),
                               min_n(range = c(min_min_n, max_min_n)), 
                               size = 25)
xgb_grid

xgb_params <- parameters(team_wf_xgb) %>%
  update(
    trees         = trees(), # uses the default range; you can override if desired
    tree_depth    = tree_depth(range = c(3, 10)),
    learn_rate    = learn_rate(range = c(0.01, 0.3)),
    loss_reduction = loss_reduction(range = c(0, 5)),
    sample_size   = sample_prop(range = c(0.5, 1.0)),
    mtry          = mtry(range = c(min_feats, max_feats)),
    min_n         = min_n(range = c(min_min_n, max_min_n))
  )
xgb_params

#--- 7) Reuse control settings from your RF code ---
control_settings <- control_grid(
  verbose = TRUE,   # prints progress
  save_pred     = TRUE,
  allow_par     = TRUE,
  parallel_over = "resamples"
)

#--- 8) Tune XGBoost over the same resamples (team_splits) & metrics ---
tic()
xgb_res_rolling <- tune_grid(team_wf_xgb, 
                             resamples = team_splits, 
                             grid = xgb_grid, 
                             metrics = metric_set(
                               accuracy, kap, roc_auc, brier_class, 
                               yardstick::spec, yardstick::sens),
                             control = control_settings)
toc()

#--- 9) Stop the parallel cluster ---
stopCluster(cl)

# Bayesian Tuning
xgb_params <- extract_parameter_set_dials(team_wf_xgb) %>%
  update(
    trees(range = c(150,1500)),
    tree_depth(range = c(1, 15)),
    learn_rate(range = c(0.001, 0.3),trans = NULL),
    loss_reduction(range = c(0, 10),trans = NULL),
    sample_prop(range = c(0.0,1)),
    mtry(range = c(min_feats, max_feats)),
    min_n(range = c(min_min_n, max_min_n))
    )

xgb_ctrl <- control_bayes(
  verbose = TRUE,
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples",
  no_improve = 3  # stop if no improvement in 4 consecutive iterations
)

tic()
set.seed(123)
xgb_rolling_bayes <- tune_bayes(
  object     = team_wf_xgb,
  resamples  = team_splits,
  param_info = xgb_params,
  initial    = 10,            # 10 random initial points
  iter       = 15,            # up to 18 Bayesian iterations
  metrics    = metric_set(accuracy,kap,roc_auc,brier_class,
                          yardstick::spec,
                          yardstick::sens),
  control = xgb_ctrl)
toc()
#  Save Random Forest Tuning Results
saveRDS(xgb_res_rolling, file = paste0(rds_files_path, "/Data/team_rocv_res_xgb_fit_v4.rds"))
saveRDS(team_wf_xgb, file = paste0(rds_files_path, "/Data/team_wf_xgb_v4.rds"))

xgb_res_rolling <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_xgb_fit_v4.rds"))
team_wf_xgb <- readRDS(paste0(rds_files_path, "/Data/team_wf_xgb_v4.rds"))
gc()

best_xgb_rolling <- select_best(xgb_res_rolling, metric = "roc_auc")
best_mtry <- best_xgb_rolling %>% pull(mtry)
best_min_n <- best_xgb_rolling %>% pull(min_n)
team_metrics <- xgb_res_rolling %>% collect_metrics(summarize = FALSE)%>%
  filter(.metric != "kap") %>%
  filter(.metric != "brier_class")

# Plotting metrics over rolling splits
ggplot(team_metrics, aes(x = id, y = .estimate, color = .metric, group = .metric)) +
  geom_line(size = 1.2) + geom_point(size = 2) +
  labs(
    title = "XGBoost Model Metrics Over Rolling Splits",
    x = "Rolling Splits", y = "Metric Estimate", color = "Metric") +
  theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1))

team_metrics_best <- team_metrics %>% 
  # filter(id != "Slice001") %>% #for comparing to meta model
  filter(mtry == best_mtry, min_n == best_min_n)

team_metrics_avg <- team_metrics_best %>%
  group_by(.metric) %>%
  summarize(mean_est = mean(.estimate, na.rm = TRUE), .groups = "drop")
team_metrics_avg 

ggplot(team_metrics_best, aes(x = id, y = .estimate, color = .metric, group = .metric)) +
  geom_line(size = 1.2) + geom_point(size = 2) +
  # Add horizontal mean lines for each metric
  geom_hline(
    data = team_metrics_avg,
    aes(yintercept = mean_est, color = .metric),
    linetype = "dashed", size = 1
  ) +
  labs(
    title = "ROC AUC Performance for Best Hyperparameters Across Rolling Splits",
    x = "Rolling Splits", y = "ROC AUC Estimate") +
  theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Final Model Evaluation with last_fit()
final_xgb_wf <- finalize_workflow(team_wf_xgb, best_xgb_rolling)
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens)
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
final_xgb_fit <- final_xgb_wf %>%
  last_fit(final_split, 
           metrics = final_metrics_set)

#  Collect and Save Final Metrics and Predictions
final_xgb_metrics <- collect_metrics(final_xgb_fit)
final_xgb_predictions <- collect_predictions(final_xgb_fit)
print(final_xgb_metrics)

saveRDS(final_xgb_wf, file = paste0(rds_files_path, "/Data/team_final_wf_xgb_v3.rds"))
saveRDS(final_xgb_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_fit_v3.rds"))
saveRDS(final_xgb_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_metrics_v3.rds"))
saveRDS(final_xgb_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_predictions_v3.rds"))

rm(final_xgb_fit)
rm(final_xgb_metrics)
rm(final_xgb_predictions)
rm(xgb_res_rolling)
gc()

## ----MODEL CALIBRATION (XGBoost)
rds_files_path <- getwd()
team_wf_xgb<- readRDS(paste0(rds_files_path,"/Data/team_wf_xgb_v3.rds"))
team_fit <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_xgb_fit_v3.rds"))
final_predictions <- readRDS(paste0(rds_files_path, "/Data/team_rocv_final_xgb_predictions_v3.rds"))
final_xgb_metrics <- readRDS(paste0(rds_files_path, "/Data/team_rocv_final_xgb_metrics_v3.rds"))
gc()


# Define metric set once
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class,
                                yardstick::spec,yardstick::sens)


### ----------Visualize Calibration (Distribution of Predictions)-----------------
#Collect all out-of-sample predictions fit_resamples() result
set.seed(123)
best_params <- select_best(team_fit, metric = "roc_auc")
print(best_params)
cv_preds <- team_fit %>% collect_predictions() %>%
  filter(.config == best_params$.config)

cv_preds_clean <- cv_preds %>%
  filter(is.finite(.pred_1),      # remove NA/NaN/Inf
    .pred_1 >= 0, .pred_1 <= 1)
rm(cv_preds)

# Set sample size, ensuring you don't exceed available rows
sample_size <-  round(nrow(cv_preds_clean))
n_rows <- nrow(cv_preds_clean)
cv_preds_sample <- cv_preds_clean %>% 
  slice_sample(n = min(n_rows, sample_size))

# Pre-Cal Facet Plot (Train Data)
xgb_facet <- cv_preds_sample %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Game Won (XGBoost, Resamples)")
xgb_facet

# Pre-Cal Resample (accuracy plot)
cal_plot_breaks(cv_preds_sample, truth = game_won,
                estimate = .pred_1, event_level = "first")
# Pre-Cal Test (accuracy plot)
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

# Beta-Cal Train Data (accuracy plot)
train_beta_cal_samp <- train_beta_cal %>% 
  slice_sample(n = min(n_rows, sample_size))
train_beta_cal_samp %>%
  cal_plot_windowed(truth = game_won, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

# Beta-Cal Facet Plot (Train Data)
xgb_train_facet <- train_beta_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Calibrated Probability Estimate of Game Won (XGBoost, Resamples)")
xgb_train_facet

rm(team_fit)
### -----Predict on the Final Holdout Split------------------
# Apply the calibration model to final predictions
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
final_cal_preds <- final_predictions %>% cal_apply(cv_cal_mod)
final_fit <- assessment(final_split) %>% rename(game_won_actual = game_won) %>%
  bind_cols(final_cal_preds)
final_metrics_1 <- final_fit %>% cls_met(game_won_actual, .pred_1)
print(final_metrics_1)

# Beta-Cal Test Data (accuracy plot)
final_cal_samp <- final_cal_preds %>% 
  slice_sample(n = min(n_rows, sample_size))
final_cal_samp %>% cal_plot_windowed(truth = game_won, estimate = .pred_1, 
                                     event_level = "first", step_size = 0.025)
# Beta-Cal Facet Plot (Test Data)
xgb_test_facet <- final_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Calibrated Probability Estimate of Point Earned (XGBoost, Test)")
xgb_test_facet

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_fit_cal.rds"))
saveRDS(final_metrics_1, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_metrics_cal.rds"))
gc()


## ----PREDICT ON FUTURE GAMES--------------------------------------------------
# Assume you already have these objects saved from your prior work:
rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
final_xgb_wf <- readRDS(paste0(rds_files_path, "/Data/team_final_wf_xgb_v3.rds"))

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Refit final model on correct training window-----
team_df_train <- team_df_played %>%
  arrange(startTimeUTC, game_id) %>%
  group_by(game_id) %>%
  dplyr::slice(1) %>%  # one row per game (assuming team-based rows)
  ungroup() %>%
  tail(3788) %>%
  pull(game_id)

# Filter training rows from team_df_played
team_df_played2 <- team_df_played %>% filter(game_id %in% team_df_train)
length(unique(team_df_played2$game_id)) #Sanity check

final_model <- fit(final_xgb_wf, data = team_df_played)
train_preds <- predict(final_model, new_data = team_df_played, type = "prob")
train_class <- predict(final_model, new_data = team_df_played) 
train_model <- train_preds %>% bind_cols(train_class) %>%
  bind_cols(team_df_played %>% select(game_won))
cv_cal_mod <- cal_estimate_beta(train_model, truth = game_won,
                                estimate = .pred_1)
team_df_cal_preds <- train_model %>% cal_apply(cv_cal_mod)

saveRDS(team_df_cal_preds, file = paste0(rds_files_path, "/Data/team_deployable_model_preds_xgb.rds"))
rm(train_preds, train_class)

# save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_xgb.rds"))
rm(final_xgb_wf)
