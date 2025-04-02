# ======= Start of Virtual Environment Setup =======

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

setwd("C:/Users/schne/OneDrive/Job & Career/Grad School/SMU/Classes/STAT 6341/Project/M3/hockey_project_v2/users/willschneider/Hockey Project")
rds_files_path <- getwd()
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_spread.rds"))
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v2.rds"))
hu_s <- dim(juice(prep(team_recipe)))[2]
team_df_played <- team_df_played %>% filter(season != 2020)

### ----ROLLING CV (250 GAME SPLIT)-----
# Create a game-level data frame
game_level_df <- team_df_played %>%
  distinct(game_id, game_date, startTimeUTC) %>%
  arrange(startTimeUTC, game_id) %>%
  mutate(game_index = row_number())

# Create rolling origin resamples at the game level
game_splits <- rolling_origin(
  data = game_level_df,
  initial = 3611,   # Approx. _ season
  assess = 250,      # Approx. _ games in the test set
  cumulative = TRUE,
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

saveRDS(final_split, file = paste0(rds_files_path, "/Data/team_final_split_250_spread.rds"))
saveRDS(team_splits, file = paste0(rds_files_path, "/Data/team_splits_250_spread.rds"))
rm(final_split)

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

set.seed(123)
# ---------------------------
# Define MLP (Neural Network) Model
# ---------------------------
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
    layer_dropout(rate = .2) %>%  # Dropout layer
    # Third Hidden Layer
    layer_dense(
      units = round(floor(hidden_units/2)),
      activation = 'relu', kernel_regularizer = regularizer_l2(l2 = penalty)) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = 0.2) %>%  # Dropout layer
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

# Using a Multi-Layer Perceptron with tuneable parameters
# Adjust the parameters (hidden_units, penalty, epochs) as needed
team_mlp <- mlp(
  hidden_units = tune(),
  penalty = tune(),
  epochs = tune())  %>% # fixed for now, can also be tuned
  set_engine("keras", 
             build_fn = build_custom_mlp,
             validation_split = 0.2,
             # learning_rate = tune(),
             callbacks = list(early_stop, reduce_lr),
             batch_size = 16) %>% # 🧠 Pass batch size here
  set_mode("classification")

# ---------------------------
# Create Workflow
# ---------------------------
team_wf_mlp <- workflow() %>%
  add_recipe(team_recipe) %>%
  add_model(team_mlp)

# # ---------------------------
# # Define Hyperparameter Grid for Neural Network
# # ---------------------------
mlp_param <- parameters(team_mlp)
mlp_param %>% extract_parameter_dials("hidden_units")
mlp_param %>% extract_parameter_dials("penalty")
mlp_param %>% extract_parameter_dials("epochs")
#### For Normal Application
# # For illustration, a small grid:
# # hidden_units: number of units in the hidden layer
# # penalty: L2 regularization for weights
# mlp_grid <- grid_space_filling(#mlp_param,
#                          hidden_units(range = c(round(floor(sqrt(hu_s-1))), (hu_s-1)*2)),
#                          penalty(range = c(-10, 0), trans = transform_log10()),
#                          epochs(range = c(35,250)),
#                          # dropout(range = c(0.1, 0.5)),
#                          # learning_rate(range = c(1e-4, 1e-2), trans = log10_trans()),
#                          size = 65)
# unique(mlp_grid$hidden_units)
# unique(mlp_grid$penalty)
# unique(mlp_grid$epochs)

### For Bayesian Application
mlp_param <- parameters(team_mlp) %>%
  update(
    hidden_units = hidden_units(range = c(round(sqrt(hu_s - 1)), (hu_s - 1) * 2)),
    # penalty = penalty(range = c(-10, 0), trans = log10_trans()),
    penalty = penalty(range = c(-6, -2), trans = log10_trans()),
    epochs = epochs(range = c(35, 250))
  )
mlp_param %>% extract_parameter_dials("hidden_units")
mlp_param %>% extract_parameter_dials("penalty")
mlp_param %>% extract_parameter_dials("epochs")

# # ---------------------------
# # Define Grid Control
# # ---------------------------
control_settings <- control_bayes(
  save_pred = TRUE,
  verbose = TRUE,
  no_improve = 15          # more tolerant to small improvements
)

#If after 10 consecutive iterations there's no improvement, optimization stops earl
# 
# # ---------------------------
# # Fit Resamples
# # ---------------------------
# mlp_res_rolling <- tune_grid(
#   team_wf_mlp,
#   resamples = team_splits,
#   grid = mlp_grid,
#   metrics = metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens),
#   control = control_settings
# )

#First, 10 initial random iterations are tested to broadly explore the hyperparameter space.
#Then, up to 30 additional iterations adaptively focus around promising regions.
mlp_res_rolling  <- tune_bayes(
  team_wf_mlp,
  resamples = team_splits,
  param_info = mlp_param,
  metrics = metric_set(roc_auc, accuracy, kap, brier_class, yardstick::spec, yardstick::sens),
  initial = 10,   # starts with 10 random points
  iter = 30,      # 30 additional Bayesian iterations
  control = control_settings
)
rm(team_splits)
gc()

###DELETE
# saveRDS(mlp_res_rolling, file = paste0(rds_files_path, "/Data/team_rocv_res_mlp_cumulative_fit_spread.rds"))
# saveRDS(team_wf_mlp, file = paste0(rds_files_path, "/Data/team_wf_mlp_cumulative_spread.rds"))
# gc()

# ---------------------------
# Save MLP Tuning Results
# ---------------------------
saveRDS(mlp_res_rolling, file = paste0(rds_files_path, "/Data/team_rocv_res_mlp_fit_spread.rds"))
saveRDS(team_wf_mlp, file = paste0(rds_files_path, "/Data/team_wf_mlp_spread.rds"))
gc()

# ---------------------------
# Select Best Hyperparameters
# ---------------------------
set.seed(123)
mlp_res_rolling <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_mlp_fit_spread.rds"))
team_wf_mlp <- readRDS(paste0(rds_files_path, "/Data/team_wf_mlp_spread.rds"))
best_mlp_rolling <- select_best(mlp_res_rolling, metric = "roc_auc")
gc()

team_metrics <- mlp_res_rolling %>% collect_metrics(summarize = FALSE) %>%
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

rm(mlp_res_rolling)
gc()

best_penalty <- best_mlp_rolling %>% pull(penalty)
best_hidden_units <- best_mlp_rolling %>% pull(hidden_units)
best_epochs<- best_mlp_rolling %>% pull(epochs)

team_metrics_best <- team_metrics %>% 
  filter(hidden_units == best_hidden_units, penalty == best_penalty, epochs == best_epochs)

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
# Finalize the Workflow
# ---------------------------
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_250_spread.rds"))
final_mlp_wf <- finalize_workflow(team_wf_mlp, best_mlp_rolling)
final_mlp_wf$fit$actions$model$spec
rm(best_mlp_rolling,team_wf_mlp)
gc()

# ---------------------------
# Final Model Evaluation with last_fit()
# ---------------------------
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens)
final_mlp_fit <- final_mlp_wf %>%
  last_fit(final_split,
           metrics = final_metrics_set)

# ---------------------------
# Collect and Save Final Metrics and Predictions
# ---------------------------
final_mlp_metrics <- collect_metrics(final_mlp_fit)
print(final_mlp_metrics)

final_mlp_predictions <- collect_predictions(final_mlp_fit)

saveRDS(final_mlp_wf, file = paste0(rds_files_path, "/Data/team_final_wf_mlp_spread.rds"))
saveRDS(final_mlp_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_fit_spread.rds"))
saveRDS(final_mlp_metrics, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_metrics_spread.rds"))
saveRDS(final_mlp_predictions, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_predictions_spread.rds"))

gc()


## ----MODEL CALIBRATION (NEURAL NET)----------------------------------------------------------------------------------------------------------------------------------
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
final_wf_mlp<- readRDS(paste0(rds_files_path,"/Data/team_wf_mlp_spread.rds"))
mlp_res_rolling <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_mlp_fit_spread.rds"))
final_predictions <- readRDS(paste0(rds_files_path, "/Data/team_rocv_final_mlp_predictions_spread.rds"))
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
cv_preds <- mlp_res_rolling %>% 
  collect_predictions() %>% 
  inner_join(best_mlp_rolling, by = c("hidden_units", "penalty",".config"))

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
pen_facet <- cv_preds_sample %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won_spread, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
pen_facet

# Resample
cal_plot_breaks(cv_preds_sample, truth = game_won_spread,
                estimate = .pred_1, event_level = "first")
# Test
cal_plot_breaks(final_mlp_predictions, truth = game_won_spread, 
                estimate = .pred_1, event_level = "first")


### ------Calibrate the Model on the training data---------------------
cv_preds_clean <- cv_preds_clean %>%
  filter(!is.na(.pred_1), !is.na(game_won_spread)) %>%
  mutate(.pred_1 = pmin(pmax(.pred_1, 0.00001), 0.99999))

cv_cal_mod <- cal_estimate_beta(cv_preds_clean, truth = game_won_spread)
train_beta_cal <- cv_preds_clean %>% cal_apply(cv_cal_mod)
cls_met <- metric_set(roc_auc, brier_class)
oth_met <- metric_set(yardstick::specificity, yardstick::sensitivity)
train_beta_cal %>% cls_met(game_won_spread, .pred_1)

train_beta_cal_samp <- train_beta_cal %>% 
  slice_sample(n = min(n_rows, sample_size))
train_beta_cal_samp %>%
  cal_plot_windowed(truth = game_won_spread, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

pen_train_facet <- train_beta_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won_spread, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
pen_train_facet

rm(team_fit)

### -----Predict on the Final Holdout Split------------------
# Apply the calibration model to final predictions
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_250_spread.rds"))

final_cal_preds <- final_mlp_predictions %>% cal_apply(cv_cal_mod)
final_fit <- assessment(final_split) %>% 
  rename(game_won_spread_actual = game_won_spread) %>%
  bind_cols(final_cal_preds)

final_metrics_1 <- final_fit %>% cls_met(game_won_spread_actual, .pred_1)
print(final_metrics_1)

final_cal_samp <- final_cal_preds %>% 
  slice_sample(n = min(n_rows, sample_size))
final_cal_samp %>%
  cal_plot_windowed(truth = game_won_spread, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

pen_test_facet <- final_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ game_won_spread, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
pen_test_facet

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_fit_cal_spread.rds"))
saveRDS(final_metrics_1, file = paste0(rds_files_path, "/Data/team_rocv_final_mlp_metrics_cal_spread.rds"))
rm(final_wf_mlp)
gc()


# ----PREDICT ON FUTURE GAMES-----------------------------------------------------------------------------------------------------------------------------------------------
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
    layer_dropout(rate = .225) %>%  # Dropout layer
    # Third Hidden Layer
    layer_dense(
      units = round(floor(hidden_units/2)),
      activation = 'relu', kernel_regularizer = regularizer_l2(l2 = penalty)) %>%
    layer_batch_normalization() %>% 
    layer_dropout(rate = 0.15) %>%  # Dropout layer
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

set.seed(123)
num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

final_model <- fit(final_mlp_wf, data = team_df_played)
# Optionally, save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/team_deployable_model_mlp_spread.rds"))

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
         model_version = "nn_v_25",   # define this near the top of your script
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

### -----Step 4: After Games Are Completed, Update the Log with Actual Outcomes------
# (This code would run in a scheduled job after outcomes are known.)
library(dplyr)
library(lubridate)

team_df <- readRDS(paste0(rds_files_path, "/Data/team_df_v2.rds"))

# Filter unplayed_results so that only games where current time is at least 1 day past startTimeUTC are updated.
unplayed_updated <- unplayed_results %>%
  filter(Sys.time() > as.POSIXct(startTimeUTC, tz = "America/Chicago") + days(1)) %>%
  left_join(team_df[, c("game_id", "teamId", "game_won_spread")],
            by = c("game_id", "teamId"))

rm(team_df)
saveRDS(unplayed_updated, file = paste0(rds_files_path, "/Data/team_unplayed_games_actuals_spread.rds"))

# Periodically evaluate performance:
library(yardstick)
metrics <- log_df_updated %>%
  metrics(truth = actual_outcome, estimate = pred_label)
print(metrics)

# Visualize trends using a dashboard or plot:
library(ggplot2)
ggplot(log_df_updated, aes(x = prediction_time, y = pred_probability)) +
  geom_line() +
  geom_point(aes(color = factor(actual_outcome))) +
  labs(title = "Prediction Trends Over Time",
       x = "Time", y = "Predicted Probability")