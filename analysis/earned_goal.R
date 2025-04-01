# --EARNED GOAL - Preprocessing and Recipe------------------------------------------------------------------------------------------------------------------------------
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
all_boxscore_df <- readRDS(paste0(rds_files_path, "/Data/combined_2009_2024_boxscore.rds"))

# Create a new outcome variable 'earned_point'
player_df <- all_boxscore_df

player_df <- player_df %>%
  mutate(earned_point = factor(earned_point, levels = c("1", "0")),
         earned_goal = factor(earned_goal, levels = c("1", "0")),
         earned_assist = factor(earned_assist, levels = c("1", "0")),)%>%
  filter(!(main_position == "goalie")) %>%
  filter(season >= "2019")

# List of columns to remove
columns_to_remove <- c( "goals", "assists", "points",
                        "plusMinus", "pim", "hits", "powerPlayGoals", "sog","faceoffWinningPctg",
                        "blockedShots", "shifts", "giveaways", "takeaways", "otInUse", "league",
                        "shootoutInUse","toi", "toi_real", "home_score", "team_goals",
                        "away_score", "med_shift_toi_per_game", 
                        "avg_shift_toi_per_game", "away_lat", "away_long", "home_lat", "home_long",
                        "venue_lat", "venue_long", "sweaterNumber", "cumulative_toi",
                        "name.default",  "easternUTCOffset",
                        "team_assists", "team_plusMinus", "team_pim", "team_hits",
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
                        "rolling_adjusted_bridge_range_add_subtract_25","n_shifts")

columns_to_remove <- unique(columns_to_remove)
common_columns <- intersect(columns_to_remove, colnames(player_df))
rm(all_boxscore_df)

# Ensure player_df is sorted by season and game_date
player_df <- player_df %>%
  arrange(season, startTimeUTC) %>%
  mutate(earned_point = as.factor(earned_point)) %>%
  mutate(earned_point = as.factor(earned_goal)) %>%
  mutate(earned_point = as.factor(earned_assist)) %>%
  drop_na() %>%
  select(-all_of(common_columns))


saveRDS(player_df,file = paste0(rds_files_path, "/Data/player_df.rds"))
player_df <- readRDS(paste0(rds_files_path, "/Data/player_df.rds"))
player_df_played <- player_df %>% filter(game_status == "played")

# Create a game-level data frame
game_level_df <- player_df_played %>%
  distinct(game_id, game_date, startTimeUTC) %>%
  arrange(startTimeUTC) %>%
  mutate(game_index = row_number())

# Create rolling origin resamples at the game level
game_splits <- rolling_origin(
  data = game_level_df,
  initial = 3878,   # Approx. _ season
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
  train_indices <- which(player_df_played$startTimeUTC %in% train_dates)
  test_indices <- which(player_df_played$startTimeUTC %in% test_dates)
  
  rsample::make_splits(
    list(analysis = train_indices, assessment = test_indices),
    data = player_df_played
  )
}

# Set up parallel backend
num_cores <- detectCores()
cl <- makeCluster(max(0,num_cores-2))
registerDoParallel(cl)

# Step 1: Translate all game-level splits into player-level splits
player_splits_list <- map(game_splits$splits, translate_splits)

# Step 4: Create the rset object for the remaining splits
player_splits <- rsample::manual_rset(
  splits = player_splits_list,
  ids = game_splits$id
)
final_split <- player_splits$splits[[dim(player_splits)[1]]]
player_splits <- player_splits[-dim(player_splits)[1],]

# Ensure player_splits is a valid rset object
class(player_splits) <- c("manual_rset", "rset", "tbl_df", "tbl", "data.frame")
rm(player_splits_list)

for (s in 1:dim(player_splits)[1]) {
  #Check logic working
  first_split <- player_splits$splits[[s]]
  train_indices <- analysis(first_split)
  test_indices <- assessment(first_split)
  
  print(paste0("NA's in ",s,"train split: ",sum(colSums(is.na(train_indices)))))
  print(paste0("NA's in ",s,"test split: ",sum(colSums(is.na(test_indices)))))
  
}

rm(train_indices)
rm(test_indices)
rm(first_split)
# Extract the first training set from rolling origin splits
initial_split <- player_splits$splits[[1]]
initial_train <- analysis(initial_split)

# # Create a recipe for initial training set and apply step_corr
# initial_recipe <- recipe(earned_point ~ ., data = initial_train) %>%
#   step_zv() %>%
#   update_role(game_id, home_id, away_id, teamId, playerId, opp_teamId,
#               new_role = "ID") %>%
#   update_role(game_date, new_role = "DATE") %>%
#   # step_rm(!!!common_columns) %>%
#   step_corr(all_numeric_predictors(), threshold = 0.95) %>%
#   prep(training = initial_train, retain = TRUE)
#   # step_naomit()
#
# # Get the names of the remaining features
# selected_features <- initial_recipe %>%
#   prep %>%
#   juice() %>%
#   colnames()
#
# player_df <- player_df %>%
#   select(all_of(selected_features))

rm(initial_split)
rm(initial_train)
rm(initial_recipe)
rm(game_level_df)

# Define recipe, model, and workflow
player_recipe <- recipe(earned_goal ~ ., data = player_df_played) %>%
  step_rm(earned_point,earned_assist,game_status) %>%
  # step_rm(earned_assist) %>%
  # step_rm(game_status) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id"))) %>%
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, playerId, opp_teamId,
              new_role = "ID") %>%
  update_role(game_date, new_role = "DATE") %>%
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  step_mutate(season = as.factor(season)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel() %>%
  step_dummy(all_nominal_predictors())  %>%
  step_smotenc(earned_goal, neighbors = 3)
# step_naomit()


stopCluster(cl)
gc()


saveRDS(player_df_played, file = paste0(rds_files_path, "/Data/player_df_played.rds"))
saveRDS(player_recipe, file = paste0(rds_files_path, "/Data/player_recipe_goal.rds"))
saveRDS(final_split, file = paste0(rds_files_path, "/Data/final_split.rds"))
saveRDS(player_splits, file = paste0(rds_files_path, "/Data/player_splits.rds"))
rm(player_recipe, final_split, player_splits,game_splits)
gc()


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
player_recipe <- readRDS(paste0(rds_files_path, "/Data/player_recipe_goal.rds"))
player_splits <- readRDS(paste0(rds_files_path, "/Data/player_splits.rds"))

gc()

# Set up parallel backend
num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

player_log <- logistic_reg() %>% 
  set_mode("classification") %>% 
  set_engine("glm")

player_wf_log <- workflow() %>% 
  add_recipe(player_recipe) %>% 
  add_model(player_log)


###-----Define Resample Control-----

control_settings <- control_resamples(
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples" #ONLY CHOOSE EVERYTHING IF COMPUTATION RESOURCES AVAILABLE!!!!
)

start_t <- Sys.time()
#Now, fit_resamples using the player-level rolling splits
player_fit <- fit_resamples(
  player_wf_log,
  resamples = player_splits,  
  metrics = metric_set(accuracy, kap, roc_auc, brier_class,yardstick::spec, yardstick::sens),
  control = control_settings)
end_t <- Sys.time()

end_t - start_t

#Save Memory
rm(player_splits)
gc()

# Collect metrics from rolling CV
saveRDS(player_wf_log, file = paste0(rds_files_path, "/Data/goal_wf_log.rds"))
saveRDS(player_fit, file = paste0(rds_files_path, "/Data/goal_rocv_res_glm_fit.rds"))

player_fit %>% collect_metrics()

#Save Memory
rm(player_fit, player_recipe)
gc()

###----Final Model Evaluation with last_fit()----
# Define metric set for final evaluation
# Extract the Last Split for Final Evaluation
# final_train <- analysis(final_split)
# final_test <- assessment(final_split)

registerDoParallel(cl)
final_split <- readRDS(paste0(rds_files_path, "/Data/final_split.rds"))
final_metrics_set <- metric_set(
  accuracy, 
  kap, 
  roc_auc, 
  brier_class, 
  yardstick::spec, 
  yardstick::sens)

# Perform final fit and evaluation
final_fit <- player_wf_log %>%
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
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/goal_rocv_final_glm_fit.rds"))
saveRDS(final_metrics, file = paste0(rds_files_path, "/Data/goal_rocv_final_glm_metrics.rds"))
saveRDS(final_predictions, file = paste0(rds_files_path, "/Data/goal_rocv_final_glm_predictions.rds"))


###-----Inspect Final Metrics and Predictions-----
# View final metrics
print(final_metrics)
head(final_predictions) 
gc()


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
player_wf_log<- readRDS(paste0(rds_files_path,"/Data/goal_wf_log.rds"))
player_fit <- readRDS(paste0(rds_files_path, "/Data/goal_rocv_res_glm_fit.rds"))
final_predictions <- readRDS(paste0(rds_files_path, "/Data/goal_rocv_final_glm_predictions.rds"))
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
cv_preds <- collect_predictions(player_fit)
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
  facet_wrap(~ earned_goal, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
log_facet

# Resample
cal_plot_breaks(cv_preds_sample, truth = earned_goal, 
                estimate = .pred_1, event_level = "first")
# Test
cal_plot_breaks(final_predictions, truth = earned_goal, 
                estimate = .pred_1, event_level = "first")


### ------Calibrate the Model on the training data---------------------
cv_cal_mod <- cal_estimate_beta(cv_preds_clean, truth = earned_goal,
                                estimate = .pred_1)
train_beta_cal <- cv_preds_clean %>% cal_apply(cv_cal_mod)
cls_met <- metric_set(roc_auc, brier_class)
oth_met <- metric_set(yardstick::specificity, yardstick::sensitivity)
train_beta_cal %>% cls_met(earned_goal, .pred_1)

train_beta_cal_samp <- train_beta_cal %>% 
  slice_sample(n = min(n_rows, sample_size))
train_beta_cal_samp %>%
  cal_plot_windowed(truth = earned_goal, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

log_train_facet <- train_beta_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ earned_goal, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
log_train_facet

rm(player_fit)
### -----Predict on the Final Holdout Split------------------
# Apply the calibration model to final predictions
final_split <- readRDS(paste0(rds_files_path, "/Data/final_split.rds"))

final_cal_preds <- final_predictions %>% cal_apply(cv_cal_mod)
final_fit <- assessment(final_split) %>% 
  rename(earned_goal_actual = earned_goal) %>%
  bind_cols(final_cal_preds)

final_metrics_1 <- final_fit %>% cls_met(earned_goal_actual, .pred_1)
print(final_metrics_1)
# final_metrics_2 <- final_fit %>% oth_met(earned_goal, .pred_1)
# print(final_metrics_2)

final_cal_samp <- final_cal_preds %>% 
  slice_sample(n = min(n_rows, sample_size))
final_cal_samp %>%
  cal_plot_windowed(truth = earned_goal, estimate = .pred_1, 
                    event_level = "first", step_size = 0.025)

log_test_facet <- final_cal_samp %>%
  ggplot(aes(.pred_1)) +
  geom_histogram(col = "white", bins = 40) +
  facet_wrap(~ earned_goal, ncol = 1, scales = "free") +
  geom_rug(col = "blue", alpha = 0.5) + 
  theme_bw() +
  labs(x = "Probability Estimate of Point Earned (Logistic, Test)")
log_test_facet

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/goal_rocv_final_glm_fit_cal.rds"))
saveRDS(final_metrics_1, file = paste0(rds_files_path, "/Data/goal_rocv_final_glm_metrics_cal.rds"))
rm(player_wf_log)
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
player_df_played <- readRDS(paste0(rds_files_path, "/Data/player_df_played.rds"))
player_wf_log <- readRDS(paste0(rds_files_path, "/Data/goal_wf_log.rds"))

num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)

### -----Step 1: Fit your workflow on all training data for deployment-------------
# This creates a fully fitted model using all of player_df_played
final_model <- fit(player_wf_log, data = player_df_played)
# Optionally, save this deployable model:
saveRDS(final_model, file = paste0(rds_files_path, "/Data/final_deployable_model.rds"))
rm(player_wf_log)

train_preds <- predict(final_model, new_data = player_df_played, type = "prob")
train_class <- predict(final_model, new_data = player_df_played) 
train_model <- train_preds %>%
  bind_cols(train_class) %>%
  bind_cols(player_df_played %>% select(earned_goal))

cv_cal_mod <- cal_estimate_beta(train_model, truth = earned_goal,
                                estimate = .pred_1)

rm(player_df_played, train_preds, train_class)
### -----Step 2: Preprocess the unplayed games data-----------------
unplayed_games <- readRDS(paste0(rds_files_path, "/Data/player_df.rds")) %>% filter(game_status == "unplayed")
all_boxscore_df <- readRDS(paste0(rds_files_path, "/Data/combined_2009_2024_boxscore.rds"))

### ---# Step 3: Predict on Unplayed Games Using the Fully Fitted Model----
# Since final_model is a fully fitted workflow, you can call predict() directly on new data.
unplayed_predictions <- predict(final_model, new_data = unplayed_games, type = "prob")
unplayed_class <- predict(final_model, new_data = unplayed_games)
unplayed_cal_preds <- unplayed_predictions  %>% cal_apply(cv_cal_mod)

player_names <- all_boxscore_df %>%
  group_by(playerId) %>%
  summarise(name.default = first(name.default), .groups = "drop")

stopCluster(cl)

# Combine predictions with unplayed games for further use (e.g., logging, analysis)
unplayed_results <- unplayed_games %>%
  mutate(pred_probability = unplayed_predictions$.pred_1,
         pred_class = unplayed_class$.pred_class,
         cal_probability = unplayed_cal_preds$.pred_1,
         prediction_time = Sys.time(),
         # Initially, actual outcome is unknown
         actual_outcome = NA) %>%
  select(-c("earned_goal","earned_assist","earned_point")) %>%
  left_join(player_names, by = "playerId")

# Save predictions
saveRDS(unplayed_results, file = paste0(rds_files_path, "/Data/unplayed_games_predictions.rds"))
saveRDS(unplayed_cal_preds, file = paste0(rds_files_path, "/Data/unplayed_games_preds_cal.rds"))
rm(unplayed_class, unplayed_cal_preds, unplayed_predictions,final_model)


#### -----Update Deployment Log------
log_file <- paste0(rds_files_path, "/Data/goal_unplayed_predictions_glm.csv")

# Define key columns and prediction columns
key_cols <- c("game_id", "playerId", "teamId")
pred_cols <- c("pred_probability", "pred_class", "cal_probability")

if (file.exists(log_file)) {
  # Read the existing log
  old_log <- read.csv(log_file, stringsAsFactors = FALSE)
  
  # Keep rows from new predictions that are different on keys and prediction columns
  new_to_add <- unplayed_results %>%
    # Use a join that compares both keys and prediction columns
    anti_join(old_log, by = c(key_cols, pred_cols))
  
  # Append only the new/changed rows to the existing log
  updated_log <- bind_rows(old_log, new_to_add)
  
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

all_boxscore_df <- readRDS(paste0(rds_files_path, "/Data/combined_2009_2024_boxscore.rds"))

# Filter unplayed_results so that only games where current time is at least 1 day past startTimeUTC are updated.
unplayed_updated <- unplayed_results %>%
  filter(Sys.time() > as.POSIXct(startTimeUTC, tz = "America/Chicago") + days(1)) %>%
  left_join(all_boxscore_df[, c("game_id", "playerId", "teamId", 
                                "earned_goal", "earned_assist", "earned_point")],
            by = c("game_id", "playerId", "teamId"))

rm(all_boxscore_df)
saveRDS(unplayed_updated, file = paste0(rds_files_path, "/Data/unplayed_games_actuals.rds"))

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


## -PENALIZED REGRESSION-------------------------------------------------------------------------------------------------------------------------------------
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
player_df_played <- readRDS(paste0(rds_files_path, "/Data/player_df.rds")) %>%
  filter(game_status == "played")
player_splits <- readRDS(paste0(rds_files_path, "/Data/player_splits.rds"))
gc()

# Define recipe, model, and workflow
player_recipe2 <- recipe(earned_goal ~ ., data = player_df_played) %>%
  step_rm(earned_point,earned_assist,game_status) %>%
  # step_rm(earned_assist) %>%
  # step_rm(game_status) %>%
  step_rm(all_of(c("venueUTCOffset","venueLocation","away_team_name", 
                   "away_team_locale","home_team_name", "home_team_locale", 
                   "winning_team","winning_team_id"))) %>%
  # Assign specific roles to ID columns
  update_role(game_id, home_id, away_id, teamId, playerId, opp_teamId,
              new_role = "ID") %>%
  update_role(game_date, new_role = "DATE") %>%
  update_role(startTimeUTC, new_role = "DATETIME") %>%
  step_mutate(season = as.factor(season)) %>%
  step_zv() %>%
  step_normalize(all_numeric_predictors()) %>%
  step_novel() %>%
  step_dummy(all_nominal_predictors())  #%>%
# step_smotenc(earned_goal, neighbors = 3)
# step_naomit()
saveRDS(player_recipe2, file = paste0(rds_files_path, "/Data/player_recipe_goal_ml.rds"))

# Set up parallel backend
num_cores <- detectCores()
cl <- makeCluster(max(1,num_cores-4))
registerDoParallel(cl)


#### -----Define Model----
# Penalized Regression for Rolling CV
player_glmnet <- logistic_reg(penalty = tune(), mixture = tune()) %>% 
  set_mode("classification") %>%
  set_engine("glmnet")

#### -----Create Workflow-----
player_wf_glmnet <- workflow() %>%
  add_recipe(player_recipe2) %>%
  add_model(player_glmnet)
rm(player_recipe2)

#### ------Define Hyperparameter Grids-----
glmnet_grid <- grid_regular(
  penalty(range = c(-4, -1), trans = log10_trans()),  # Log scale for penalty
  mixture(range = c(0, 1)),                           # Mixture between Ridge and Lasso
  levels = 5
)


#### --------Define Grid Control--------
control_settings <- control_grid(
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples"
)


#### --------Fit Resamples--------
# Tuning sets for glmnet on rolling CV
glmnet_fit <- tune_grid(
  player_wf_glmnet,
  resamples = player_splits,
  grid = glmnet_grid,    # reuse the same grid if appropriate
  metrics = metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens),
  control = control_settings
)

#Save Memory
rm(player_splits)
gc()

# Collect metrics from rolling CV
saveRDS(player_wf_glmnet, file = paste0(rds_files_path, "/Data/goal_wf_glmnet.rds"))
saveRDS(glmnet_fit, file = paste0(rds_files_path, "/Data/goal_rocv_res_glmnet_fit.rds"))
glmnet_fit <- readRDS(paste0(rds_files_path, "/Data/goal_rocv_res_glmnet_fit.rds"))

glmnet_fit %>% collect_metrics()
stopCluster(cl)


#### ---------------------------Select Best Hyperparameters for glmnet---------------------------
best_glmnet <- select_best(glmnet_fit, metric = "sens")
#Save Memory
rm(glmnet_fit)
gc()

#### ---------------------------Finalize Workflow for glmnet---------------------------
final_glmnet_wf <- finalize_workflow(player_wf_glmnet, best_glmnet)

#### ---------------------------Final Model Evaluation with last_fit()---------------------------
# Define metric set for final evaluation
final_metrics_set <- metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens)

#### ---------------------------Final Model Evaluation with last_fit() for glmnet---------------------------
final_split <- readRDS(paste0(rds_files_path, "/Data/final_split.rds"))
# Perform final fit and evaluation
final_fit <- final_glmnet_wf %>%
  last_fit(final_split, 
           metrics = final_metrics_set)

#### ---------------------------Collect and Save Final Metrics and Predictions---------------------------
# Collect final metrics
final_metrics <- collect_metrics(final_fit)

# Collect final predictions
final_predictions <- collect_predictions(final_fit)
stopCluster(cl)

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/goal_rocv_final_glmnet_fit.rds"))
saveRDS(final_metrics, file = paste0(rds_files_path, "/Data/goal_rocv_final_glmnet_metrics.rds"))
saveRDS(final_predictions, file = paste0(rds_files_path, "/Data/goal_rocv_final_glmnet_predictions.rds"))


###-----Inspect Final Metrics and Predictions-----
# View final metrics
print(final_metrics)
head(final_predictions) 
gc()