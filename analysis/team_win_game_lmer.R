library(tidymodels)
library(multilevelmod)  # makes sure that "lmer", "glmer", etc. are available
library(lme4)
library(tidyverse)
library(doParallel)

rds_files_path <- getwd()
team_recipe_lmer <- readRDS(paste0(rds_files_path, "/Data/team_recipe_lmer.rds"))
team_splits <- readRDS(paste0(rds_files_path, "/Data/team_splits.rds"))
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played.rds"))

# Define recipe, model, and workflow
team_recipe_lmer <- recipe(game_won ~ ., data = team_df_played) %>%
  step_rm(game_status) %>%
  step_rm(playerId,matches("25"),matches("15"),venueUTCOffset,away_team_name, 
          venueLocation, away_team_locale,home_team_name, home_team_locale, 
          winning_team,winning_team_id, game_id, game_date, startTimeUTC, 
          home_id, away_id, opp_teamId) %>%
  step_mutate(gameType = as.factor(gameType)) %>%
  step_mutate(teamId = as.factor(teamId)) %>%
  step_mutate(is_home = as.factor(is_home)) %>%
  # step_mutate(season = as.factor(season)) %>%
  # Ensure 'season' has fixed levels
  step_mutate(season = factor(season, levels = c("X2020", "X2021", "X2022", "X2023", "X2024"))) %>%
  step_dummy(all_nominal_predictors(), -c(teamId)) %>%
  step_normalize(all_numeric_predictors(), -c(teamId)) %>%
  step_zv(all_predictors(), -c(teamId)) %>%
  step_novel()

vars <- team_recipe_lmer$var_info
rec_bake <- team_recipe_lmer %>% prep() %>% bake(., new_data =  NULL)
colnames(rec_bake)

saveRDS(team_recipe_lmer, file = paste0(rds_files_path, "/Data/team_recipe_lmer.rds"))
rm(team_df_played)
gc()

num_cores <-  detectCores()
cl <- max(1, num_cores - 4)
registerDoParallel(cl)

# Define the model specification using logistic_reg() and the "glmer" engine
lmer_spec <- 
  logistic_reg() %>% 
  set_mode("classification") %>% 
  set_engine("glmer", family = binomial(link = "logit"), nAGQ = 1)

# Create a workflow that uses add_variables() to explicitly set the outcome and predictors.
# For example, assume you want to include all predictors except a few nonpredictor variables.
# You can determine the predictor names programmatically (or list them manually).
# Build the workflow by adding your recipe and model
fixed_predictors <- setdiff(names(rec_bake), c("game_won", "teamId"))
fixed_effects <- paste(fixed_predictors, collapse = " + ")
full_formula <- as.formula(paste("game_won ~", fixed_effects,
                                 "+ (1 | teamId)"))
lmer_wflow <- 
  workflow() %>% 
  add_recipe(team_recipe_lmer) %>% 
  add_model(lmer_spec, 
            formula = full_formula)

control_settings <- control_resamples(
  save_pred = TRUE,
  allow_par = TRUE,
  parallel_over = "resamples" #ONLY CHOOSE EVERYTHING IF COMPUTATION RESOURCES AVAILABLE!!!!
)
rm(team_recipe_lmer)

# Then, assuming you have a resamples object (e.g., team_splits) ready:
results <- lmer_wflow %>% 
  fit_resamples(
    resamples = team_splits,  
    metrics = metric_set(accuracy, kap, roc_auc, brier_class, yardstick::spec, yardstick::sens),
    control = control_resamples(save_pred = TRUE, allow_par = TRUE)
  )

stopCluster(cl)
gc()

# Collect metrics from rolling CV
saveRDS(lmer_wflow, file = paste0(rds_files_path, "/Data/team_wf_lmer.rds"))
saveRDS(results, file = paste0(rds_files_path, "/Data/team_rocv_res_lmer_fit.rds"))

registerDoParallel(cl)
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split.rds"))
final_metrics_set <- metric_set(
  accuracy, 
  kap, 
  roc_auc, 
  brier_class, 
  yardstick::spec, 
  yardstick::sens)

# Perform final fit and evaluation
final_fit <- lmer_wflow %>%
  last_fit(final_split, 
           metrics = final_metrics_set)

###----Collect and Save Final Metrics and Predictions----
# Collect final metrics
final_metrics <- final_fit %>% collect_metrics()
print(final_metrics)

# Collect final predictions
final_predictions <- final_fit %>% collect_predictions()

# Save final metrics and predictions
saveRDS(final_fit, file = paste0(rds_files_path, "/Data/team_rocv_lmer_final_fit.rds"))
saveRDS(final_metrics, file = paste0(rds_files_path, "/Data/team_rocv_lmer_final_metrics.rds"))
saveRDS(final_predictions, file = paste0(rds_files_path, "/Data/team_rocv_lmer_final_predictions.rds"))
stopCluster(cl)

###----Quantifying Random Intercept Importance-----
final_fit <- readRDS(paste0(rds_files_path,"/Data/team_rocv_lmer_final_fit.rds"))
final_model <- final_fit %>% extract_fit_parsnip()

# Get the coefficients
res_coefficients <- tidy(final_model)
# View coefficients
print(res_coefficients)

#Extract Variance Components
vc <- lme4::VarCorr(final_model$fit)
print(vc)

# Convert the variance components to a data frame
vc_df <- as.data.frame(vc)

# Assuming the grouping factors are named "teamId", "season", and "gameType":
resid_var <- (pi^2) / 3

# Compute the proportion of total variance for each random effect
vc_df <- vc_df %>%
  mutate(proportion = vcov / (vcov + resid_var))

print(vc_df)

###----Random Slope EDA for teamId-----
rec_data <- team_recipe_lmer %>% prep() %>% bake(new_data = team_df_played) %>%
  mutate(
    travel_distance = if_else(round(is_home_X1) == 1, home_distance, away_distance),
    travel_distance_opp = if_else(round(is_home_X1) == 1, away_distance, home_distance),
    rolling_distance_5 = if_else(round(is_home_X1) == 1, rolling_home_distance_5, rolling_away_distance_5),
    rolling_distance_5_opp = if_else(round(is_home_X1) == 1, rolling_away_distance_5, rolling_home_distance_5),
    hrs_since_last_game = if_else(round(is_home_X1) == 1, home_hrs_since_last_game, away_hrs_since_last_game),
    hrs_since_last_game_opp = if_else(round(is_home_X1) == 1, away_hrs_since_last_game, home_hrs_since_last_game )
    )

ggplot(rec_data, aes(x = team_PDO_5_roll_avg_opp, y = game_won, color = teamId)) +
  geom_smooth(method = "glm", method.args = list(family = "binomial"), se = FALSE)

ggplot(rec_data, aes(x = team_PDO_5_roll_avg_opp, y = game_won, color = factor(teamId))) +
  geom_jitter(width = 0, height = 0.02, alpha = 0.3) +
  geom_smooth(method = "glm", method.args = list(family = "binomial"), se = FALSE)

rec_data %>%
  group_by(teamId) %>%
  summarise(n = n(), dist_range = range(travel_distance))

View(rec_data %>%
  sample_n(10) %>%
  select(teamId, is_home_X1, home_distance, away_distance, travel_distance, travel_distance_opp, game_won))

ggplot(rec_data, aes(x = team_PDO_5_roll_avg_opp, y = game_won)) +
  geom_jitter(width = 0, height = 0.02, alpha = 0.3) +
  geom_smooth(method = "glm", method.args = list(family = "binomial"), se = FALSE) +
  facet_wrap(~ teamId)


###---Systematic Comparison of Random Effects (Workflow changes)
full_model_fit <- extract_fit_parsnip(full_fit)$fit
red_model_fit <- extract_fit_parsnip(red_fit)$fit

AIC_full <- AIC(full_model_fit)
AIC_red <- AIC(red_model_fit)

# Workflow with all random effects
full_formula <- as.formula(paste("game_won ~", fixed_effects,
                                 "+ (1 | teamId) + (1 | season) + (1 | gameType)"))
full_wflow <- workflow() %>% 
  add_recipe(team_recipe_lmer) %>% 
  add_model(lmer_spec, formula = full_formula)

# Workflow with reduced random effects (only teamId)
red_formula <- as.formula(paste("game_won ~", fixed_effects,
                                "+ (1 | teamId)"))
red_wflow <- workflow() %>% 
  add_recipe(team_recipe_lmer) %>% 
  add_model(lmer_spec, formula = red_formula)

# Resampling each candidate
full_res <- full_wflow %>% 
  fit_resamples(resamples = team_splits, metrics = metric_set(accuracy, roc_auc))

red_res <- red_wflow %>% 
  fit_resamples(resamples = team_splits, metrics = metric_set(accuracy, roc_auc))


