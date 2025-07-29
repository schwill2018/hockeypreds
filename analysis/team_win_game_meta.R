# ======= Start of Virtual Environment Setup =======

# Optionally install Python 3.11 if not already installed.
# This installs a version of Python (via Miniconda) that reticulate can use.
# reticulate::install_python(version = "3.11.0")

# Load the reticulate package
library(reticulate)

# # Verify that the HOME environment variable is set correctly
print(path.expand("~"))  # Expected output: "C:/Users/schne"

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

# ======= End of Virtual Environment Setup =======
library(tidymodels)
library(tidyverse)
library(parsnip)
library(finetune)
library(iml)
library(gridExtra)
library(tictoc)
library(doParallel)
library(keras)
library(tensorflow)
library(dials)

setwd("C:/Users/schne/OneDrive/Grad School/SMU/Classes/STAT 6341/Project/M3/main")
rds_files_path <- getwd()
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_meta.rds"))
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits_v3.rds"))
hu_s <- dim(juice(prep(team_recipe)))[2]
shap_knee <- readRDS(paste0(rds_files_path, "/Data/team_shap_knee_drop_nn_meta.rds"))
gc()

# Update Recipe after Examining VI, ALE, and SHAP plots
# to_remove <- tail(impt_df$feature, 137) %>% str_subset("season", negate = TRUE)
to_remove <- shap_knee %>%
  str_subset("season", negate = TRUE) %>%
  str_subset("is_home", negate = TRUE) %>%
  str_subset("gameType", negate = TRUE) %>%
  str_subset("is_back_to_back", negate = TRUE) %>%
  str_subset("b2b_win_ratio_lag", negate = TRUE) %>%
  str_subset("no_feats_games", negate = TRUE) %>%
  str_subset("elo_home_pre", negate = TRUE)

patterns <- c("rolling_home_distance","rolling_away_distance","tz_diff_game",
              "home_venue_time_diff") #feature removals discovered
pattern_regex <- paste(patterns, collapse = "|")
cols_to_drop   <- grep(pattern_regex, names(team_df_played), value = TRUE)

team_recipe <- recipe(game_won ~ ., data = team_df_played) %>%
  #step_filter(gameType == 2) %>%
  # step_filter(season != 2020) %>% #Remove 2020 season (black swan data)
  step_rm(game_status) %>% 
  step_rm(game_won_spread) %>%
  step_rm(playerId) %>%
  step_rm(prior_rank, rolling_rank, rolling_points, cum_points, cum_rank) %>%
  step_rm(last_period_type) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id","venue_time", "game_time"))) %>%
  step_rm(any_of(!!cols_to_drop)) %>% #remove for full meta recipe
  step_rm(any_of(!!to_remove)) %>% #remove for full meta recipe
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, opp_teamId, new_role = "ID") %>%
  update_role(game_date, new_role = "DATE") %>% 
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  update_role(elo_class, elo_prob, new_role = "ELO") %>%
  step_mutate(is_home = as.factor(is_home)) %>%
  step_mutate(season = as.factor(season)) %>%
  step_mutate(gameType = as.factor(gameType)) %>%
  step_mutate(is_back_to_back = as.factor(is_back_to_back)) %>%
  step_mutate(no_feats_games = as.factor(no_feats_games)) %>%
  # step_mutate(elo_class = as.factor(elo_class)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel(all_nominal_predictors(), -is_home, -gameType, -is_back_to_back,
             -no_feats_games) %>%
  step_dummy(all_nominal_predictors()) 
rec_bake <- team_recipe %>% prep() %>% bake(new_data = NULL)
colnames(rec_bake)
saveRDS(team_recipe,paste0(rds_files_path, "/Data/team_recipe_trim_meta.rds"))

## Neural Net ----
# Define MLP (Neural Network) Model
set.seed(123)

# Custom model-building function
build_custom_mlp <- function(hidden_units, l1_penalty, l2_penalty, my_dropout) {
  reg <- regularizer_l1_l2(l1 = l1_penalty, l2 = l2_penalty)
  d_rate <- my_dropout
  # reg <- regularizer_l2(l2 = penalty)
  # d_rate <- 0.15
  keras_model_sequential() %>%
    layer_dense(
      units = hidden_units,
      activation = 'relu',
      input_shape = c(hu_s)-1,  # actual number of features - 1 to remove predictor
      kernel_regularizer = reg #regularizer_l2(l2 = penalty)
    ) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = d_rate) %>% # Dropout layer
    # Second Hidden Layer
    layer_dense(
      units = round(floor(hidden_units* (2/3))),
      activation = 'relu', 
      kernel_regularizer = reg #regularizer_l2(l2 = penalty)
      ) %>%
    layer_batch_normalization() %>%
    layer_dropout(rate = d_rate) %>%  # Dropout layer
    # Third Hidden Layer
    layer_dense(
      units = round(floor(sqrt(hidden_units))*2),
      activation = 'relu', 
      kernel_regularizer = reg #regularizer_l2(l2 = penalty)
      ) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = d_rate) %>%  # Dropout layer
    layer_dense(units = 1, activation = 'sigmoid') %>%
    compile(
      optimizer = optimizer_adam(learning_rate = 0.001),
      # optimizer = 'adam',
      loss = 'binary_crossentropy',
      metrics = 'binary_accuracy',
      # metrics   = c("binary_accuracy", "AUC")
      # metrics   = list(
      #   keras::metric_binary_accuracy(name = "binary_accuracy"),
      #   keras::metric_auc(name            = "auc")
      # )
    )
  }

# Define Early Stopping callback
# callback_reduce_lr_on_plateau()
early_stop <- callback_early_stopping(
  monitor = "val_loss",
  # monitor  = "val_auc",
  # mode = "max",
  patience = 7, 
  min_delta = .0001,
  # baseline = .69,
  restore_best_weights = TRUE # Restore model weights from the epoch with the best value of the monitored metric
)

reduce_lr <- callback_reduce_lr_on_plateau(
  monitor = "val_loss",
  factor = 0.5,
  patience = 2,
  min_lr = 1e-6,
  verbose = 1,
)

# Using a Multi-Layer Perceptron with tuneable parameters
# Adjust the parameters (hidden_units, penalty, epochs) as needed
team_mlp <- mlp(
  hidden_units = tune(),
  # penalty = tune(),
  epochs = 120)  %>% # fixed for now, can also be tuned
  set_engine("keras", 
             build_fn = build_custom_mlp,
             my_dropout = tune(),
             l1_penalty = tune(),
             l2_penalty = tune(),
             validation_split = .15,
             # learning_rate = tune(),
             callbacks = list(early_stop, reduce_lr),
             batch_size = 16) %>% # 🧠 Pass batch size here
  set_mode("classification")

team_wf_mlp <- workflow() %>%
  add_recipe(team_recipe) %>%
  add_model(team_mlp)


# mlp_param <- parameters(team_mlp)
# mlp_param %>% extract_parameter_dials("hidden_units")
# mlp_param %>% extract_parameter_dials("penalty")
# mlp_param %>% extract_parameter_dials("my_dropout")
# mlp_param %>% extract_parameter_dials("epochs")

# mlp_param <- parameters(team_mlp) %>%
#   update(
#     hidden_units = hidden_units(range = c(round(sqrt(hu_s - 1)), (hu_s - 1) * 2)),
#     penalty = penalty(range = c(-5, -2), trans = log10_trans())
#     # l1_penalty = penalty(range = c(-5, -2), trans = log10_trans()),
#     # l2_penalty = penalty(range = c(-5, -2), trans = log10_trans())
#     #,epochs = epochs(range = c(40, 120))
#     )

### For Bayesian Application
drop_par <- new_quant_param(
  type      = "double",
  range     = c(-5, 0),
  inclusive = c(TRUE, TRUE),
  trans     = log10_trans(),             # no transformation
  label     = c(my_dropout = "Dropout rate")
)

l1_par <- new_quant_param(
  type      = "double",
  range     = c(-5, -2),
  inclusive = c(TRUE, TRUE),
  trans     = log10_trans(),             # no transformation
  label     = c(l1_penalty = "l1")
)
l2_par <- new_quant_param(
  type      = "double",
  range     = c(-5, -2),
  inclusive = c(TRUE, TRUE),
  trans     = log10_trans(),             # no transformation
  label     = c(l2_penalty = "l2")
)

mlp_param <- parameters(
  hidden_units = hidden_units(range = c(round(sqrt(hu_s - 1)), (hu_s - 1) * 2)),
  l1_par, l2_par, drop_par
  # penalty = penalty(range = c(-5, -2), trans = log10_trans()),
)


mlp_param %>% extract_parameter_dials("hidden_units")
mlp_param %>% extract_parameter_dials("l1_penalty")
mlp_param %>% extract_parameter_dials("l2_penalty")
mlp_param %>% extract_parameter_dials("my_dropout")
# mlp_param %>% extract_parameter_dials("epochs")
# mlp_param %>% extract_parameter_dials("penalty")

# # Define Grid Control
#If after 10 consecutive iterations there's no improvement, optimization stops earl
control_settings <- control_bayes(verbose = TRUE, save_pred = TRUE, 
                                  allow_par = TRUE, parallel_over = "resamples",
                                  no_improve = 3)
rm(team_recipe)
gc()

#First, 10 initial random iterations are tested to broadly explore the hyperparameter space.
#Then, up to 30 additional iterations adaptively focus around promising regions.
tic()
mlp_res_rolling  <- tune_bayes(
  team_wf_mlp,
  resamples = team_splits,
  param_info = mlp_param,
  metrics = metric_set(roc_auc, accuracy, kap, brier_class, yardstick::spec, yardstick::sens),
  initial = 10,   # starts with 8 random points
  iter = 8,      # 8 additional Bayesian iterations
  control = control_settings
)
toc()
rm(team_splits)
gc()

# Save MLP Tuning Results
saveRDS(team_wf_mlp, file = paste0(rds_files_path, "/Data/team_wf_mlp_meta.rds"))
saveRDS(mlp_res_rolling, file = paste0(rds_files_path, "/Data/team_rocv_res_mlp_fit_meta.rds"))
gc()

# Select Best Hyperparameters
set.seed(123)
mlp_res_rolling <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_mlp_fit_meta.rds"))
team_wf_mlp <- readRDS(paste0(rds_files_path, "/Data/team_wf_mlp_meta.rds"))
best_mlp_rolling <- show_best(mlp_res_rolling, metric = "roc_auc", n = 1)
# saveRDS(best_mlp_rolling, file = paste0(rds_files_path, "/Data/team_rocv_res_mlp_best_meta.rds"))


team_metrics <- mlp_res_rolling %>% collect_metrics(summarize = FALSE) %>%
  filter(.metric != "kap") %>%
  filter(.metric != "brier_class")

best_l1 <- best_mlp_rolling %>% pull(l1_penalty)
best_l2 <- best_mlp_rolling %>% pull(l2_penalty)
best_hidden_units <- best_mlp_rolling %>% pull(hidden_units)
best_drop <- best_mlp_rolling %>% pull(my_dropout)
# best_epochs<- best_mlp_rolling %>% pull(epochs)
team_metrics_best <- team_metrics %>% 
  filter(hidden_units == best_hidden_units, 
         l1_penalty == best_l1,
         l2_penalty == best_l2, my_dropout == best_drop)
team_metrics_avg <- team_metrics_best %>%
  group_by(.metric) %>%
  summarize(mean_est = mean(.estimate, na.rm = TRUE), .groups = "drop")
team_metrics_avg

rm(mlp_res_rolling)
gc()

# Finalize the Workflow
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
final_mlp_wf <- finalize_workflow(team_wf_mlp, best_mlp_rolling)
final_mlp_wf$fit$actions$model$spec

# Final Model Evaluation with last_fit()
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens)
final_mlp_fit <- final_mlp_wf %>% last_fit(final_split, metrics = final_metrics_set)

# Collect and Save Final Metrics and Predictions
final_mlp_metrics <- collect_metrics(final_mlp_fit)
final_mlp_predictions <- collect_predictions(final_mlp_fit)
saveRDS(final_mlp_wf, file = paste0(rds_files_path, "/Data/team_final_wf_mlp_meta.rds"))
saveRDS(final_mlp_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_fit_meta.rds"))
saveRDS(final_mlp_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_metrics_meta.rds"))
saveRDS(final_mlp_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_predictions_meta.rds"))

rm(final_mlp_fit)
rm(final_mlp_metrics)
rm(final_mlp_predictions)
rm(final_mlp_wf)
rm(mlp_res_rolling)
rm(team_wf_mlp)
gc()


## GLM ----
# Set up parallel backend
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_trim_meta.rds"))
num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-3))
registerDoParallel(cl)

team_log <- logistic_reg() %>% 
  set_mode("classification") %>% 
  set_engine("glm")

team_wf_log <- workflow() %>% 
  add_recipe(team_recipe) %>% 
  add_model(team_log)

# Define Resample Control
control_settings <- control_resamples(
  verbose = TRUE,
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
team_fit %>% collect_metrics()
stopCluster(cl)
gc()
team_fit$id
# Collect metrics from rolling CV
saveRDS(team_wf_log, file = paste0(rds_files_path, "/Data/team_wf_log_meta.rds"))
saveRDS(team_fit, file = paste0(rds_files_path, "/Data/team_rocv_res_glm_fit_meta.rds"))

team_fit <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_glm_fit_meta.rds"))
team_wf_log <- readRDS(paste0(rds_files_path,"/Data/team_wf_log_meta.rds"))
# Extract and organize metrics
team_metrics <- team_fit %>% collect_metrics(summarize = FALSE) %>%
  filter(.metric != "kap") %>%
  filter(.metric != "brier_class")
gc()

#Final Model Evaluation with last_fit()
registerDoParallel(cl)
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
final_metrics_set <- metric_set(
  accuracy, 
  kap, 
  roc_auc, 
  brier_class, 
  yardstick::spec, 
  yardstick::sens)

# Perform final fit and evaluation
final_fit <- team_wf_log %>% last_fit(final_split, metrics = final_metrics_set)

# Collect and Save Final Metrics and Predictions
final_metrics <- final_fit %>% collect_metrics()
final_predictions <- final_fit %>% collect_predictions()
stopCluster(cl)

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_fit_meta.rds"))
saveRDS(final_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_metrics_meta.rds"))
saveRDS(final_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_glm_predictions_meta.rds"))
rm(team_log, team_metrics, team_fit, final_fit, final_metrics, team_wf_log, 
   final_predictions)
gc()

## Random Forest ----
library(ranger)
library(tictoc)

team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
shap_knee <- readRDS(paste0(rds_files_path, "/Data/team_shap_knee_drop_rf_meta.rds"))
to_remove <- shap_knee %>%
  str_subset("season", negate = TRUE) %>%
  str_subset("is_home", negate = TRUE) %>%
  str_subset("gameType", negate = TRUE) %>%
  str_subset("is_back_to_back", negate = TRUE) %>%
  str_subset("b2b_win_ratio_lag", negate = TRUE) %>%
  str_subset("no_feats_games", negate = TRUE) %>%
  str_subset("elo_home_pre", negate = TRUE) 
patterns <- c("rolling_home_distance","rolling_away_distance","tz_diff_game",
              "home_venue_time_diff") #feature removals discovered
pattern_regex <- paste(patterns, collapse = "|")
cols_to_drop   <- grep(pattern_regex, names(team_df_played), value = TRUE)

team_recipe <- recipe(game_won ~ ., data = team_df_played) %>%
  #step_filter(gameType == 2) %>%
  # step_filter(season != 2020) %>% #Remove 2020 season (black swan data)
  step_rm(game_status) %>% 
  step_rm(game_won_spread) %>%
  step_rm(playerId) %>%
  step_rm(prior_rank, rolling_rank, rolling_points, cum_points, cum_rank) %>%
  step_rm(last_period_type) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id","venue_time", "game_time"))) %>%
  step_rm(any_of(!!cols_to_drop)) %>%
  step_rm(any_of(!!to_remove)) %>%
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, opp_teamId, new_role = "ID") %>%
  update_role(game_date, new_role = "DATE") %>% 
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  update_role(elo_class, elo_prob, new_role = "ELO") %>%
  step_mutate(is_home = as.factor(is_home)) %>%
  step_mutate(season = as.factor(season)) %>%
  step_mutate(gameType = as.factor(gameType)) %>%
  step_mutate(is_back_to_back = as.factor(is_back_to_back)) %>%
  step_mutate(no_feats_games = as.factor(no_feats_games)) %>%
  # step_mutate(elo_class = as.factor(elo_class)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel(all_nominal_predictors(), -is_home, -gameType, -is_back_to_back,
             -no_feats_games) %>%
  step_dummy(all_nominal_predictors()) 
rec_bake <- team_recipe %>% prep() %>% bake(new_data = NULL)
colnames(rec_bake)
saveRDS(team_recipe,paste0(rds_files_path, "/Data/team_recipe_trim_rf_meta.rds"))

# Define Model
set.seed(123)
#Define Cluster for Parallel Proccessing
num_cores <- detectCores()-3
cl <- makeCluster(max(1, num_cores))
registerDoParallel(cl)

# Random Forest for Rolling CV
team_rf_rolling <- rand_forest(mtry = tune(), min_n = tune(), trees = tune())%>%
  set_mode("classification") %>%
  set_engine("ranger", importance  = "permutation")

team_wf_rf_rolling <- workflow() %>%
  add_recipe(team_recipe) %>%
  add_model(team_rf_rolling)
rec_bake <- team_recipe %>% prep() %>% bake(., new_data =  NULL)

# # Define Hyperparameter Grids for Random Forest
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
rf_params <- parameters(team_wf_rf_rolling) %>%
  update(
    mtry    = mtry(range = c(min_feats, max_feats)),
    min_n   = min_n(range = c(min_min_n, max_min_n)),
    trees   = trees(range = c(50, 500))       # adjust range to taste
  )
rf_params

set.seed(123) # reproducible
rf_grid_lh <- grid_space_filling(
  rf_params,
  type = "latin_hypercube",
  size = 12  # number of Latin-hypercube points
)
rf_grid_lh
# Define Grid Control
control_settings <- control_grid(
  verbose = TRUE,   # prints progress
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples"
)

# Fit Resamples
tic()
rf_res_rolling <- tune_grid(
  team_wf_rf_rolling, 
  resamples = team_splits,
  grid = rf_grid_lh,
  metrics = metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec,
                       yardstick::sens),
  control = control_settings)
toc()

#  Save Random Forest Tuning Results
saveRDS(rf_res_rolling, file = paste0(rds_files_path, "/Data/team_rocv_res_rf_fit_meta.rds"))
saveRDS(team_wf_rf_rolling, file = paste0(rds_files_path, "/Data/team_wf_rf_meta.rds"))
gc()

# Select Best Hyperparameters
team_metrics <- rf_res_rolling %>% collect_metrics(summarize = FALSE) %>%
  filter(.metric != "kap") %>% filter(.metric != "brier_class")
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

# 10. Finalize the Workflow
final_rf_wf <- finalize_workflow(team_wf_rf_rolling, best_rf_rolling)

# Final Model Evaluation with last_fit()
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens)
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
final_rf_fit <- final_rf_wf %>%
  last_fit(final_split, 
           metrics = final_metrics_set)

#  Collect and Save Final Metrics and Predictions
final_rf_metrics <- collect_metrics(final_rf_fit)
final_rf_predictions <- collect_predictions(final_rf_fit)

saveRDS(final_rf_wf, file = paste0(rds_files_path, "/Data/team_final_wf_rf_meta.rds"))
saveRDS(final_rf_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_fit_meta.rds"))
saveRDS(final_rf_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_metrics_meta.rds"))
saveRDS(final_rf_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_rf_predictions_meta.rds"))

rm(final_rf_wf)
rm(final_rf_fit)
rm(final_rf_metrics)
rm(final_rf_predictions)
rm(rf_res_rolling, team_wf_rf_rolling,team_rf_rolling)
stopCluster(cl)
gc()

## XGBoost ----
library(finetune)
library(themis)
library(stringr)   
library(xgboost)      
library(dplyr)        
library(parsnip)      
library(workflows)    
library(rsample)  
library(doParallel)
library(tictoc)
library(dials)
library(tidymodels)
library(tidyverse)

rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
shap_knee <- readRDS(paste0(rds_files_path, "/Data/team_shap_knee_drop_xgb_meta.rds"))
team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits_v3.rds"))
to_remove <- shap_knee %>%
  str_subset("season", negate = TRUE) %>%
  str_subset("is_home", negate = TRUE) %>%
  str_subset("gameType", negate = TRUE) %>%
  str_subset("is_back_to_back", negate = TRUE) %>%
  str_subset("b2b_win_ratio_lag", negate = TRUE) %>%
  str_subset("no_feats_games", negate = TRUE) %>%
  str_subset("elo_home_pre", negate = TRUE) 
patterns <- c("rolling_home_distance","rolling_away_distance","tz_diff_game",
              "home_venue_time_diff") #feature removals discovered
pattern_regex <- paste(patterns, collapse = "|")
cols_to_drop   <- grep(pattern_regex, names(team_df_played), value = TRUE)

team_recipe <- recipe(game_won ~ ., data = team_df_played) %>%
  #step_filter(gameType == 2) %>%
  # step_filter(season != 2020) %>% #Remove 2020 season (black swan data)
  step_rm(game_status) %>% 
  step_rm(game_won_spread) %>%
  step_rm(playerId) %>%
  step_rm(prior_rank, rolling_rank, rolling_points, cum_points, cum_rank) %>%
  step_rm(last_period_type) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id","venue_time", "game_time"))) %>%
  step_rm(any_of(!!cols_to_drop)) %>%
  step_rm(any_of(!!to_remove)) %>%
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, opp_teamId, new_role = "ID") %>%
  update_role(game_date, new_role = "DATE") %>% 
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  update_role(elo_class, elo_prob, new_role = "ELO") %>%
  step_mutate(is_home = as.factor(is_home)) %>%
  step_mutate(season = as.factor(season)) %>%
  step_mutate(gameType = as.factor(gameType)) %>%
  step_mutate(is_back_to_back = as.factor(is_back_to_back)) %>%
  step_mutate(no_feats_games = as.factor(no_feats_games)) %>%
  # step_mutate(elo_class = as.factor(elo_class)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel(all_nominal_predictors(), -is_home, -gameType, -is_back_to_back,
             -no_feats_games) %>%
  step_dummy(all_nominal_predictors()) 
rec_bake <- team_recipe %>% prep() %>% bake(new_data = NULL)
colnames(rec_bake)
saveRDS(team_recipe,paste0(rds_files_path, "/Data/team_recipe_trim_xgb_meta.rds"))
rm(team_df_played)

set.seed(123)
# 3 Compute mtry / min_n ranges based on rec_bake (same logic as RF) 
num_total_cols <- ncol(rec_bake)
num_non_pred_vars <- team_recipe$var_info %>%
  filter(role != "predictor") %>%
  pull(variable) %>%
  unique() %>%
  length()
num_predictors <- num_total_cols - num_non_pred_vars
max_feats  <- ceiling(sqrt(num_predictors))
min_feats  <- floor(log2(num_predictors)) + 1
max_min_n  <- ceiling(sqrt(3611)) 
min_min_n  <- 5                 

# 4 Define XGBoost model spec with tunable parameters
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

# 5 Workflow (recipe + model)
team_wf_xgb <- workflow() %>%
  add_recipe(team_recipe) %>%
  add_model(xgb_spec)

# 1 Parallel backend (same as RF)
num_cores <- detectCores() - 3
cl <- makeCluster(max(1, num_cores))
registerDoParallel(cl)

# 6 Define a random grid for XGBoost tuning
#    We draw 20 random combinations to keep the grid manageable.
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
                               size = 35)
xgb_grid 
# Reuse control settings from your RF code
control_settings <- control_grid(
  verbose = TRUE,   # prints progress
  save_pred     = TRUE,
  allow_par     = TRUE,
  parallel_over = "resamples")

# Tune XGBoost over the same resamples (team_splits) & metrics
tic()
xgb_res_rolling <- tune_grid(team_wf_xgb, 
                             resamples = team_splits, 
                             grid = xgb_grid, 
                             metrics = metric_set(
                               accuracy, kap, roc_auc, brier_class, 
                               yardstick::spec, yardstick::sens),
                             control = control_settings)
toc()
stopCluster(cl)

# Bayesian Tuning
set.seed(123)
num_cores <- detectCores() - 3
cl <- makeCluster(max(1, num_cores))
registerDoParallel(cl)

tic()
xgb_params <- parameters(
    trees(range = c(150,1500)),
    tree_depth(range = c(1, 15)),
    learn_rate(range = c(-3, -0.5), trans = log10_trans()),
    loss_reduction(range = c(-6, 1), trans = log10_trans()),
    sample_prop(range = c(0.5,1)),
    mtry(range = c(min_feats, max_feats)),
    min_n(range = c(min_min_n, max_min_n))
  )

xgb_ctrl <- control_bayes(
  verbose = TRUE,
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples",
  no_improve = 5  # stop if no improvement in 3 consecutive iterations
)

set.seed(123)
xgb_rolling_bayes <- tune_bayes(
  object     = team_wf_xgb,
  resamples  = team_splits,
  param_info = xgb_params,
  initial    = 15,            # 10 random initial points
  iter       = 15,            # up to 18 Bayesian iterations
  metrics    = metric_set(accuracy,kap,roc_auc,brier_class,
                          yardstick::spec,
                          yardstick::sens),
  control = xgb_ctrl)
toc()
stopCluster(cl)


#  Save Random Forest Tuning Results
saveRDS(xgb_rolling_bayes, file = paste0(rds_files_path, "/Data/team_rocv_res_xgb_fit_meta.rds"))
saveRDS(team_wf_xgb, file = paste0(rds_files_path, "/Data/team_wf_xgb_meta.rds"))
gc()

best_xgb_rolling <- select_best(xgb_rolling_bayes, metric = "roc_auc")
best_mtry <- best_xgb_rolling %>% pull(mtry)
best_min_n <- best_xgb_rolling %>% pull(min_n)
best_trees <- best_xgb_rolling %>% pull(trees)
best_tree_depth <- best_xgb_rolling %>% pull(tree_depth)
best_learn_rate <- best_xgb_rolling %>% pull(learn_rate)
best_loss_reduction <- best_xgb_rolling %>% pull(loss_reduction)
best_sample_prop <- best_xgb_rolling %>% pull(sample_size)


team_metrics <- xgb_rolling_bayes %>% collect_metrics(summarize = FALSE) %>%
  filter(.metric != "kap") %>%
  filter(.metric != "brier_class")

team_metrics_best <- team_metrics %>% 
  filter(mtry == best_mtry, min_n == best_min_n, trees == best_trees,
         tree_depth == best_tree_depth, learn_rate == best_learn_rate, 
         loss_reduction == best_loss_reduction, sample_size == best_sample_prop)

team_metrics_avg <- team_metrics_best %>%
  group_by(.metric) %>%
  summarize(mean_est = mean(.estimate, na.rm = TRUE), .groups = "drop")
team_metrics_avg 

rm(xgb_rolling_bayes)
# Final Model Evaluation with last_fit()
final_xgb_wf <- finalize_workflow(team_wf_xgb, best_xgb_rolling)
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class, 
                                yardstick::spec, yardstick::sens)
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
final_xgb_fit <- final_xgb_wf %>% last_fit(final_split, 
                                           metrics = final_metrics_set)

#  Collect and Save Final Metrics and Predictions
final_xgb_metrics <- collect_metrics(final_xgb_fit)
final_xgb_predictions <- collect_predictions(final_xgb_fit)
saveRDS(final_xgb_wf, file = paste0(rds_files_path, "/Data/team_final_wf_xgb_meta.rds"))
saveRDS(final_xgb_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_fit_meta.rds"))
saveRDS(final_xgb_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_metrics_meta.rds"))
saveRDS(final_xgb_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_xgb_predictions_meta.rds"))
rm(final_xgb_fit)
rm(final_xgb_metrics)
rm(final_xgb_predictions)
rm(xgb_res_rolling, team_wf_xgb)
gc()

## ENSEMBLE (Including ELO probabilities) ----
library(tidyverse)
library(tidymodels)
library(dplyr)
library(purrr)
library(tidyr)
library(doParallel)
library(confintr)
library(pROC)
library(dials)

rds_files_path <- getwd()
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
glm_fs_preds <- readRDS(paste0(
        rds_files_path, "/Data/team_rocv_final_glm_predictions_meta.rds")) %>%
  mutate(glm_pred = .pred_1)
rf_fs_preds <- readRDS(paste0(
        rds_files_path, "/Data/team_rocv_final_rf_predictions_meta.rds")) %>%
  mutate(rf_pred = .pred_1)
xgb_fs_preds <- readRDS(paste0(
        rds_files_path, "/Data/team_rocv_final_xgb_predictions_meta.rds")) %>%
  mutate(xgb_pred = .pred_1)
nn_fs_preds <- readRDS(paste0(
        rds_files_path, "/Data/team_rocv_final_mlp_predictions_meta.rds")) %>%
  mutate(nn_pred = .pred_1)

### ----- OPTIONAL CHECK OF ORG MODEL PREDICTORS ----- 
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_meta.rds"))
rec_bake <- team_recipe %>% prep() %>% bake(new_data = NULL)
preds <- team_recipe$var_info %>% filter(role == "predictor")

team_log <- logistic_reg() %>% 
  set_mode("classification") %>% 
  set_engine("glm")
team_wf_log <- workflow() %>% 
  add_recipe(team_recipe) %>% 
  add_model(team_log)
mold <- extract_mold(team_wf_log %>% fit(team_df_played))
colnames(mold$predictors)
# rm(mold)

### Helper Function: best .config by ROC‑AUC ---------------------------------------
best_config_id <- function(rs_obj) {
  collect_metrics(rs_obj) %>%
    filter(.metric == "roc_auc") %>%
    arrange(desc(mean)) %>%
    slice(1) %>%
    pull(.config)
}

### Collect out‑of‑fold (OOF) predictions ---------------------------------
  get_oof <- function(rs_obj, model_nm) {
    best_cfg <- select_best(rs_obj, metric = "roc_auc")$.config
    collect_predictions(rs_obj) %>%
      filter(.config == best_cfg) %>%
      mutate(!!model_nm := .pred_1) #%>%      # create new column with dynamic name
      # select(id, .row, !!model_nm)      
  }

#### Load files individually to save space ---------------------------------
#Define Cluster for Parallel Proccessing
# num_cores <- detectCores()-3
# cl <- makeCluster(max(1, num_cores))
# registerDoParallel(cl)
# 
xgb_oof <- readRDS(paste0(rds_files_path, "/Data/team_rocv_oof_xgb_predictions_meta.rds"))
rf_oof <- readRDS(paste0(rds_files_path, "/Data/team_rocv_oof_rf_predictions_meta.rds"))
nn_oof <- readRDS(paste0(rds_files_path, "/Data/team_rocv_oof_nn_predictions_meta.rds"))
glm_oof <- readRDS(paste0(rds_files_path, "/Data/team_rocv_oof_glm_predictions_meta.rds"))

# rf_oof <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_rf_fit_meta.rds")) %>% get_oof( "rf_pred")
# # rm(rf_rs)
# gc()
# 
# xgb_oof <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_xgb_fit_meta.rds")) %>% get_oof("xgb_pred")
# # rm(xgb_rs)
# gc()
# 
# # nn_rs
# nn_oof <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_mlp_fit_meta.rds")) %>% get_oof("nn_pred")
# # rm(nn_rs)
# gc()
# 
# glm_rs  <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_glm_fit_meta.rds"))
# refit_data <- glm_oof[[1]][[1]][[1]]
# # glm_oof <- glm_rs %>% get_oof("glm_pred")
# rm(glm_rs)
# gc()
# 
# library(lobstr)
# tree(glm_oof, max_depth = 3)  
# # # rm(glm_rs)
# # gc()
# # stopCluster(cl)

### Fold‑truth frame: outcome + Elo prob ----------------------------------
#### Load files individually to save space ---------------------------------
team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits_v3.rds"))
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
team_splits[[1]][[102]][[3]]
final_split[[3]]
rm(team_splits,final_split)
gc()

# !! Set your outcome column here !!
response_col <- "game_won"   # <‑‑ change to the actual outcome variable name

### Truth + Elo for every observation --------------------------------
meta_base <- team_df_played %>%
  mutate(.row = row_number()) %>%
  select(.row, elo_prob, truth = all_of(response_col))

### Assemble meta‑training set --------------------------------------------
# glm_oof <- glm_oof %>% select(names(glm_fs_preds)) %>% rbind(glm_fs_preds)
# rf_oof <- rf_oof %>% select(names(rf_fs_preds)) %>% rbind(rf_fs_preds)
# xgb_oof <- xgb_oof %>% select(names(xgb_fs_preds))  %>% rbind(xgb_fs_preds)
# nn_oof <- nn_oof %>% select(names(nn_fs_preds)) %>% rbind(nn_fs_preds)
# 
# saveRDS(glm_oof, file = paste0(rds_files_path, "/Data/team_rocv_oof_glm_predictions_meta.rds"))
# saveRDS(rf_oof, file = paste0(rds_files_path, "/Data/team_rocv_oof_rf_predictions_meta.rds"))
# saveRDS(nn_oof, file = paste0(rds_files_path, "/Data/team_rocv_oof_nn_predictions_meta.rds"))
# saveRDS(xgb_oof, file = paste0(rds_files_path, "/Data/team_rocv_oof_xgb_predictions_meta.rds"))

meta_train <- reduce(list(glm_oof %>% select(.row, id, matches("_pred")),
                          rf_oof %>% select(.row, id, matches("_pred")),
                          xgb_oof %>% select(.row, id, matches("_pred")),
                          nn_oof %>% select(.row, id, matches("_pred"))),
                    left_join, by = c("id",".row")) %>%
              left_join(meta_base, ".row") #%>% 
              # filter(id != "Slice103")

### Fit logistic meta‑model ------------------------------------------------
meta_rec <- recipe(truth ~ ., meta_train) %>%
            update_role(.row, new_role = "ROW") %>%
            update_role(id, new_role = "ID")
meta_wf <- workflow() %>%
  add_recipe(meta_rec) %>%
  add_model(logistic_reg() %>% set_engine("glm"))

### Chronological leave‑one‑split‑out evaluation ----------------------
# We must ensure the meta‑model trains only on *past* splits.
# We'll map each `id` to its original position in `team_splits$id`.
id_order <- tibble(id = unique(meta_train$id), idx = seq_along(id)) #%>%
  # add_row(id = "train/test split", idx = (dim(team_splits)[1]+ 1)) # format final split
meta_train <- left_join(meta_train, id_order, by = "id")
saveRDS(meta_train, file = paste0(rds_files_path, "/Data/team_rocv_oof_predictions_all.rds"))
rm(team_splits, final_split)

cv_results <- map_dfr(unique(meta_train$idx), function(test_idx) {
  if (test_idx == 1) return(NULL)
  train_rows <- filter(meta_train, idx < test_idx)   # strictly earlier splits
  test_rows  <- filter(meta_train, idx == test_idx)  # current split only
  if(nrow(train_rows) == 0 || nrow(test_rows) == 0) return(NULL)
  meta_fit_tmp <- fit(meta_wf, data = train_rows)
  test_prob    <- predict(meta_fit_tmp, test_rows, type = "prob")$.pred_1
  test_rows %>%
    mutate(stack_prob = test_prob) %>% select(id, idx, .row, stack_prob, truth)  
})
saveRDS(cv_results, file = paste0(rds_files_path, "/Data/team_rocv_oof_predictions_meta.rds"))
### Plot ensemble metrics across rolling splits --------------------------
library(ggplot2)
cv_results <- cv_results %>% filter(id != "Slice001")
auto_metrics <- cv_results %>%
  mutate(pred_class = factor(if_else(stack_prob >= 0.50, 1,0), levels = c(1,0))) %>%
  group_by(id) %>%
  summarise(accuracy = yardstick::accuracy_vec(truth, pred_class),
            roc_auc = yardstick::roc_auc_vec(truth, stack_prob),
            brier_class = yardstick::brier_class_vec(truth, stack_prob),
            sens = yardstick::sensitivity_vec(truth, pred_class),
            spec = yardstick::specificity_vec(truth, pred_class),
            .groups = "drop") %>%
  pivot_longer(-id, names_to = ".metric", values_to = ".estimate")
auto_metrics_avg <- auto_metrics %>% group_by(.metric) %>%
  summarise(mean_est = mean(.estimate, na.rm = TRUE), .groups = "drop")

ggplot(auto_metrics, aes(x = id, y = .estimate, color = .metric, group = .metric)) +
  geom_line(size = 1.2) + geom_point(size = 2) +
  # Add horizontal mean lines for each metric
  geom_hline(data = auto_metrics_avg,
             aes(yintercept = mean_est, color = .metric),
             linetype = "dashed", size = 1) +
  labs(
    title = "Meta-Model ROC AUC for Best Hyperparameters Across Rolling Splits",
    x = "Rolling Splits", y = "ROC AUC Estimate") +
  theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1))

### Compute ROC‑AUC per split for **all** models ----------------------
meta_roc <- roc_auc(cv_results, truth, stack_prob)
#Remove 1st split from Org models (b/c Ensemble doesnt use)
#Combine last split with resample splits
glm_oof_n1 <- glm_oof %>% filter(id != "Slice001")
rf_oof_n1 <- rf_oof %>% filter(id != "Slice001")
xgb_oof_n1 <- xgb_oof %>% filter(id != "Slice001")
nn_oof_n1 <- nn_oof %>% filter(id != "Slice001")
glm_roc <- roc_auc(glm_oof_n1, game_won, .pred_1)
rf_roc <- roc_auc(rf_oof_n1, game_won, .pred_1)
xgb_roc <- roc_auc(xgb_oof_n1, game_won, .pred_1)
nn_roc <- roc_auc(nn_oof_n1, game_won, .pred_1)
elo_roc <- roc_auc(team_df_played, game_won, elo_prob)
glm_roc
rf_roc
xgb_roc
nn_roc
elo_roc
meta_roc
rm(glm_oof, rf_oof, xgb_oof, nn_oof)

roc_by_split <- bind_rows(
  glm_oof_n1 %>% group_by(id) %>% summarise(.estimate = yardstick::roc_auc_vec(game_won, glm_pred), .groups = "drop") %>% mutate(model="GLM"),
  rf_oof_n1  %>% group_by(id) %>% summarise(.estimate = yardstick::roc_auc_vec(game_won, rf_pred ), .groups = "drop") %>% mutate(model="RF" ),
  xgb_oof_n1 %>% group_by(id) %>% summarise(.estimate = yardstick::roc_auc_vec(game_won, xgb_pred), .groups = "drop") %>% mutate(model="XGB"),
  nn_oof_n1  %>% group_by(id) %>% summarise(.estimate = yardstick::roc_auc_vec(game_won, nn_pred ), .groups = "drop") %>% mutate(model="NN" ),
  cv_results %>% group_by(id) %>% summarise(.estimate = yardstick::roc_auc_vec(truth, stack_prob), .groups = "drop") %>% mutate(model="STACK")
)

### Plot ROC‑AUC over splits ----------------------------------------
mean_by_model <- roc_by_split %>%
  group_by(model) %>%
  summarise(mean_auc = mean(.estimate, na.rm = TRUE), .groups = "drop")

ggplot(roc_by_split, aes(x = id, y = .estimate, color = model, group = model)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_hline(data = mean_by_model, aes(yintercept = mean_auc, color = model),
             linetype = "dashed", linewidth = 1) +
  labs(title = "ROC AUC Performance Across Rolling Splits — All Models",
       x = "Rolling Split", y = "ROC‑AUC", color = "Model") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

saveRDS(roc_by_split, file = paste0(rds_files_path, "/images/meta_roc_by_split.rds"))
getwd()
### Evaluate ROC-AUC Confidence -----------
library(pROC)

ci <- ci.auc(response = cv_results$truth,
             predictor = cv_results$stack_prob,
             boot.n   = 2000, 
             conf.level = 0.95)     # returns 3-element vector
ci
