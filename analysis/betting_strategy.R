# ---- LOAD LIBRARIES ----------------------------------------------------------------------
library(tidyverse)
library(tidyr)
library(lubridate)
library(tidymodels)
library(ggplot2)


# ---- PREP & CLEAN DATA ---------------------------------------------------------------------
rds_files_path <- getwd()
f_path <- getwd()
team_df_played <- readRDS(paste0(f_path, "/Data/team_df_played_v3.rds"))
meta_results <- readRDS(paste0(rds_files_path, "/Data/team_rocv_oof_predictions_meta.rds"))
all_results <- readRDS(paste0(rds_files_path, "/Data/team_rocv_oof_predictions_all.rds"))

## ---- MODEL DATA ---------------------------------------------------------------------
# combined_log_path <- file.path(rds_files_path, "Data/team_unplayed_combined_predictions.csv")
# combined_log <- read.csv(combined_log_path)
combined_log <- team_df_played %>% 
  mutate(row_num = row_number()) %>% 
  left_join(meta_results, by = c("row_num" = ".row")) %>% 
  left_join(all_results %>% select(-elo_prob), 
            by = c("row_num" = ".row", "id", "idx","truth")) %>%
  filter(season == 2024) %>% drop_na()

# Convert Time Stamps
combined_log <- combined_log %>%
  filter(season == 2024) %>%
  mutate(
    # prob_class = ifelse(stack_prob >= .50, 1, 0), 
    # cal_class = ifelse(cal_probability >= .50, 1, 0), 
    # startTimeUTC_curr = as.POSIXct(startTimeUTC_curr, 
    #                                format = "%Y-%m-%dT%H:%M:%SZ",
    #                                tz = "UTC"), # "2025-04-01T00:30:00Z"
    # prediction_time = as.POSIXct(prediction_time, 
    #                              format = "%Y-%m-%dT%H:%M:%SZ",
    #                              tz = "UTC"), # "2025-04-01T00:30:00Z"
    startTimeUTC = as.POSIXct(startTimeUTC,
                              format = "%Y-%m-%d %H:%M:%S",
                              tz = "UTC"), # "2025-04-01 00:30:00"
    game_date = as.Date(game_date),
    # model_date = as.Date(startTimeUTC_curr),
    home_team = paste0(home_team_locale, " ",home_team_name),
    away_team = paste0(away_team_locale, " ",away_team_name),
    team_name = case_when(
      teamId == home_id ~ paste0(home_team_locale, " ",home_team_name),
      teamId == away_id ~ paste0(away_team_locale, " ",away_team_name))) %>%
  mutate(home_team = case_when(
    home_team == "Utah Utah Hockey Club" ~ "Utah Hockey Club",
    home_team == "St. Louis Blues" ~ "St Louis Blues",
    TRUE ~ home_team),
    away_team = case_when(
      away_team == "Utah Utah Hockey Club" ~ "Utah Hockey Club",
      away_team == "St. Louis Blues" ~ "St Louis Blues",
      TRUE ~ away_team),
    team_name = case_when(
      team_name == "Utah Utah Hockey Club" ~ "Utah Hockey Club",
      team_name == "St. Louis Blues" ~ "St Louis Blues",
      TRUE ~ team_name)) 

#one row per game-model pair 
prob_long <- combined_log %>% 
  pivot_longer(  # 1. reshape wide → long
    cols = c(glm_pred, rf_pred, xgb_pred, nn_pred, stack_prob, elo_prob),
    names_to  = "model_raw", values_to = "prob_pred") %>% 
  mutate(  # 2. give tidy model labels
    model = recode(model_raw, glm_pred = "log_model", rf_pred = "rf_model",
                   xgb_pred = "xgboost_model", nn_pred = "neural_net",
                   stack_prob = "meta_model", elo_prob = "elo_model"),
    pred_class = ifelse(prob_pred >= .50, 1, 0)) %>% 
  select(-model_raw)     

#Get most recent predictions (ones prior to game)
latest_scores <- prob_long
# latest_scores <- combined_log #%>%
  # filter(!is.na(prediction_time)) %>% 
  # filter(prediction_time <= startTimeUTC_curr) %>%
  # group_by(model_version, game_id, teamId) %>%
  # slice_max(order_by = prediction_time, n = 1, with_ties = FALSE) %>%
  # ungroup()

# unique(latest_scores$model_version)
length(unique(latest_scores$game_id))

# 1) Filter to completed games and add y, y_hat:
df <- latest_scores %>% 
  # filter(!is.na(actual_outcome)) %>%
  filter(!is.na(game_won)) %>%
  mutate(
    y = as.numeric(as.character(game_won)),
    y_hat = as.integer(pred_class),         # ensure 0/1
    p_hat = prob_pred)

# 1.a) Build a count table of (model_version, actual, predicted):
conf_df <- df %>%
  count(y, y_hat, model) %>% #model_version,
  complete(
    model,
    y = c(1, 0),
    y_hat = c(1, 0),
    fill = list(n = 0)) %>%
  mutate(Actual = factor(y,   levels = c(1,0), labels = c("True 1","True 0")),
    Predicted = factor(y_hat, levels = c(1,0), labels = c("Pred 1","Pred 0"))) #%>%
  #filter(model_version != "v_25")
getwd()
saveRDS(conf_df, file = paste0(getwd(), "/images/confusion_df.rds"))
# 1.b) Plot a faceted confusion matrix (counts) per model_version:
ggplot(conf_df, aes(x = Predicted, y = Actual, fill = n)) +
  geom_tile(color = "white") +
  geom_text(aes(label = n), size = 5) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  facet_wrap(~model) +
  labs(
    x = "Predicted", 
    y = "Actual",
    title = "Confusion Matrix by Model Version"
  ) +
  theme_minimal()

ggplot(conf_df %>% filter(model == "rf_model"), aes(x = Predicted, y = Actual, fill = n)) +
  geom_tile(color = "white") +
  geom_text(aes(label = n), size = 5) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  facet_wrap(~ model) +
  labs(
    x = "Predicted",
    y = "Actual",
    title = "Confusion Matrix by Model Version"
  ) +
  theme_minimal()

models <- unique(latest_scores$model) #Extract all unique model_version values:
# 2) Define a helper to compute metrics for one model
compute_metrics <- function(sub_df) {
  N  <- nrow(sub_df)
  TP <- sum(sub_df$y == 1 & sub_df$y_hat == 1)
  TN <- sum(sub_df$y == 0 & sub_df$y_hat == 0)
  FP <- sum(sub_df$y == 0 & sub_df$y_hat == 1)
  FN <- sum(sub_df$y == 1 & sub_df$y_hat == 0)
  
  # Performance Metrics
  accuracy <- (TP + TN) / N
  sensitivity <- if ((TP + FN) > 0) TP / (TP + FN) else NA_real_
  specificity <- if ((TN + FP) > 0) TN / (TN + FP) else NA_real_
  precision <- if ((TP + FP) > 0) TP / (TP + FP) else NA_real_
  brier <- mean((sub_df$p_hat - sub_df$y)^2)
  # Cohen's Kappa
  P_yes_true  <- (TP + FN) / N
  P_no_true   <- (TN + FP) / N
  P_yes_pred  <- (TP + FP) / N
  P_no_pred   <- (TN + FN) / N
  P_o <- (TP + TN) / N
  P_e <- P_yes_true * P_yes_pred + P_no_true * P_no_pred
  kappa <- if ((1 - P_e) > 0) (P_o - P_e) / (1 - P_e) else NA_real_
  
  # ROC AUC by pairwise comparison
  pos_idx <- which(sub_df$y == 1)
  neg_idx <- which(sub_df$y == 0)
  wins <- 0
  ties <- 0
  for (i in pos_idx) {
    for (j in neg_idx) {
      if (sub_df$p_hat[i] > sub_df$p_hat[j]) {
        wins <- wins + 1
      } else if (sub_df$p_hat[i] == sub_df$p_hat[j]) {
        ties <- ties + 1
      }
    }
  }
  num_pairs <- length(pos_idx) * length(neg_idx)
  roc_auc <- if (num_pairs > 0) (wins + 0.5 * ties) / num_pairs else NA_real_
  
  tibble(
    Accuracy    = accuracy,
    Sensitivity = sensitivity,
    Specificity = specificity,
    Brier       = brier,
    Kappa       = kappa,
    ROC_AUC     = roc_auc,
    Precision   = precision
  )
}

# 4) Loop over each model_version and bind results:
results_list <- lapply(models, function(m) {
  df_sub <- filter(df, model == m)
  metrics <- compute_metrics(df_sub)
  bind_cols(model_version = m, metrics)
})
metrics_by_model <- bind_rows(results_list)

# 5) Inspect:
metrics_by_model %>% filter(model_version != "v_25")

## ---- ODDS DATA ----------------------------------------------------------------------
f_path <- getwd()
log_file <- file.path(f_path, "Data", "nhl_team_odds_log.csv")
odds <- readRDS(paste0(f_path, "/Data/odds_data_20242025.rds"))
offtime_odds <- odds %>% filter(commence_time < timestamp)
#Check timestamps are before games
nrow(odds) - nrow(offtime_odds) 

odds <- odds %>%
  group_by(id, markets_key, key, title, markets_last_update) %>%
  filter(n() == 2) %>%  # Only keep games with exactly 2 rows (one per team)
  arrange(desc(implied_prob)) %>%
  mutate(side = c("fav", "dog")) %>%
  ungroup() %>%
  pivot_wider(
    id_cols = c(id, markets_key, key, title, markets_last_update,
                commence_time, away_team, home_team,),
    names_from = side,
    values_from = c(name, price, implied_prob),
    names_glue = "{.value}_{side}") %>%
  #Calcualte Dec Odds & VIG Adj
  mutate(dec_odds_fav = (100/abs(price_fav)) + 1,
         dec_odds_dog = (price_dog/100) + 1,
         adj_implied_prob_fav = implied_prob_fav / (implied_prob_fav + implied_prob_dog),
         adj_implied_prob_dog = implied_prob_dog / (implied_prob_fav + implied_prob_dog),
         vig = (implied_prob_fav + implied_prob_dog - 1)
         ) %>%
  pivot_longer(
    cols = c(name_fav, name_dog, price_fav, price_dog, implied_prob_fav, 
             implied_prob_dog, dec_odds_fav, dec_odds_dog, 
             adj_implied_prob_fav, adj_implied_prob_dog),
    names_to = c(".value", "side"),
    names_pattern = "^(.*)_(fav|dog)$") %>%
  group_by(id, markets_key) %>%
  mutate(vig_book_spread = max(vig) - min(vig),
         min_book_prob = min(adj_implied_prob, na.rm = TRUE),
         max_book_prob = max(adj_implied_prob, na.rm = TRUE)) %>% 
  mutate(
    commence_time = as.POSIXct(commence_time, format = "%Y-%m-%dT%H:%M:%SZ",
                               tz = "UTC"), # "2025-03-31T19:25:18Z"s
    markets_last_update = as.POSIXct(markets_last_update,
                           format = "%Y-%m-%dT%H:%M:%SZ",
                           tz = "UTC"), # "2025-04-01T00:30:00Z"
    odds_date = as.Date(commence_time)) %>%
  ungroup()
            

latest_odds <- odds %>%
  filter(!is.na(markets_last_update)) %>% 
  filter((markets_last_update <= commence_time)) %>% #(pull_time <= commence_time) &
  group_by(id,key, side) %>%
  slice_max(order_by = markets_last_update, n = 1, with_ties = FALSE) %>%
  ungroup()

## ---- JOIN MODEL LOG AND ODDS ---------
#Check to see team names are the same
setdiff(unique(combined_log$home_team), unique(odds$home_team))
setdiff(unique(combined_log$away_team), unique(odds$away_team))
setdiff(unique(combined_log$team_name), unique(odds$name))

#Create a joined dataframe
# joined1 <- latest_scores %>%
#   left_join(latest_odds %>% rename(odds_id = id), 
#             by = c("home_team", "away_team", 
#                    "team_name" = "name",
#                    "startTimeUTC" = "commence_time"))
# 
# unmatched1 <- joined1 %>% #Extract rows still unmatched after stage 1
#   filter(is.na(price)) %>%
#   select(names(latest_scores))
# 
# joined2 <- unmatched1 %>%
#   left_join(latest_odds %>% rename(odds_id = id), 
#             by = c("home_team", "away_team", "team_name" = "name",
#                    "startTimeUTC" = "commence_time",
#                    "game_date" = "odds_date"))
# 
# unmatched2 <- joined2 %>% #still unmatched after stage 2
#   filter(is.na(price)) %>%
#   select(names(latest_scores))
# 
# joined3 <- unmatched2 %>%
#   left_join(latest_odds, by = c("home_team", "away_team", 
#                                 "team_name" = "name",
#                                 "model_date" = "odds_date"))
# 
# unmatched3 <- joined3 %>% #still unmatched after stage 2
#   filter(is.na(price)) %>%
#   select(names(latest_scores))

#Create a joined dataframe
joined1 <- latest_scores %>%
  left_join(latest_odds %>% rename(odds_id = id), by = c("home_team", "away_team", 
                                "team_name" = "name",
                                "startTimeUTC" = "commence_time",
                                "game_date" = "odds_date")) 
unmatched1 <- joined1 %>% #Extract rows still unmatched after stage 1
  filter(is.na(price)) %>%
  select(names(latest_scores))

joined2 <- unmatched1 %>% 
  left_join(latest_odds %>% rename(odds_id = id), 
            by = c("home_team", "away_team", "team_name" = "name",
                   "game_date" = "odds_date"))

unmatched2 <- joined2 %>% #still unmatched after stage 2
  filter(is.na(price)) %>%
  select(names(latest_scores))

joined3 <- unmatched2 %>%
  left_join(latest_odds %>% rename(odds_id = id), 
            by = c("home_team", "away_team", "team_name" = "name",
                   "startTimeUTC" = "commence_time"))

unmatched3 <- joined3 %>% #still unmatched after stage 2
  filter(is.na(price)) %>%
  select(names(latest_scores))

# 8. Combine:
matched1 <- joined1 %>% filter(!is.na(price))
dim(matched1)
sum(is.na(matched1))
matched2 <- joined2 %>% select(-commence_time) %>% filter(!is.na(price))
dim(matched2)
sum(is.na(matched2))
matched3 <- joined3 %>% select(-odds_date) %>% filter(!is.na(price)) 
dim(matched3)
sum(is.na(matched3))

final_df <- bind_rows(matched1, matched2, matched3) %>%
  mutate(model_edge = prob_pred - adj_implied_prob,
         vig_buffer = implied_prob - adj_implied_prob,
         exp_val = (prob_pred * dec_odds) - 1,
         model_pick = exp_val > 0) %>%
  mutate(prob_diff = prob_pred - adj_implied_prob) #%>%
  # filter(markets_last_update > prediction_time) 

# final_df <- latest_odds
# 9. (Optional) Check how many still lack odds:
still_unmatched <- final_df %>%
  filter(is.na(price))

# ---- PRELIMINARY ANALYSES ----

## ---- MARKET STRUCTURE -------
### ---- VIG ACROSS GAMES (PER BOOKMAKER) - HIST -----
ggplot(final_df, aes(x = vig)) +
  geom_histogram(binwidth = 0.0005, color = "black", fill = "#4C72B0") +
  facet_wrap(~ key, ncol = 2) +
  labs(title = "Histograms by Group",
       x = "Value",
       y = "Count") +
  theme_minimal()

ggplot(final_df, aes(x = vig, fill = key)) +
  geom_histogram(alpha = 0.5, position = "identity", binwidth = .0005) +
  labs(title = "Overlaid Histograms",
       x = "Value",
       y = "Count") +
  theme_classic()

### ---- VIG SPREAD PER GAME (MULTI-BOOK) -----
ggplot(final_df, aes(x = vig_book_spread)) +
  geom_histogram(binwidth = 0.0025, color = "black", fill = "#DD8452") +
  labs(title = "Distribution of Vig Spread Across Books (per Game)",
       x = "Vig Spread (Max - Min)",
       y = "Number of Games") +
  theme_minimal()


### ---- VIG BY BOOKMAKER - BOX -----
ggplot(final_df, aes(x = vig, y = key)) +
  geom_boxplot(fill = "#55A868") +
  labs(title = "Vig by Bookmaker",
       x = "Bookmaker",
       y = "Vig") +
  theme_minimal()

### ---- VIG BUFFER BY TEAM -----
# 1) Bar‐chart of average vig buffer per team
team_vig_avg <- final_df %>%
  group_by(team_name) %>%
  summarise(avg_vig_buffer = mean(vig_buffer, na.rm = TRUE)) %>%
  arrange(desc(avg_vig_buffer))

ggplot(team_vig_avg, aes(x = reorder(team_name, avg_vig_buffer), y = avg_vig_buffer)) +
  geom_col(fill = "#E69F00") +
  coord_flip() +
  labs(
    title = "Average Vig Buffer by Team",
    x = "Team",
    y = "Avg(raw implied − vig-free implied)"
  ) +
  theme_minimal()
# 2) Boxplot of vig buffer distribution per team
ggplot(final_df, aes(x = reorder(team_name, vig_buffer, median), y = vig_buffer)) +
  geom_boxplot(fill = "#56B4E9") +
  coord_flip() +
  labs(
    title = "Distribution of Vig Buffer by Team",
    x = "Team",
    y = "Raw vs Vig-free Implied Prob Difference"
  ) +
  theme_minimal()

### ---- MIN/MAX IMP. PROBS PER TEAM-----
team_prob_summary <- final_df %>%
  group_by(id, team_name) %>%
  summarise(prob_spread = max(implied_prob, na.rm = TRUE) - 
              min(implied_prob, na.rm = TRUE), .groups = "drop")

ggplot(team_prob_summary, 
       aes(x = reorder(team_name, prob_spread, median),
           y = prob_spread)) +
  geom_boxplot(fill = "#4C72B0") +
  coord_flip() +
  labs(title = "Implied Probability Spread by Team (Across Games)",
       x = "Team",
       y = "Spread (Max - Min)") +
  theme_minimal()

## ----- EXPECTED VALUE --------
ggplot(final_df %>% filter(exp_val > 0), aes(x = exp_val)) +
  geom_histogram(binwidth = 0.0025, color = "black", fill = "#DD8452") +
  labs(title = "Distribution of Expected Value per Bet",
       x = "Exp Val",
       y = "Number of Games") +
  theme_minimal()


### ---- EXPECT VALUE BY BOOKMAKER - BOX -----
ggplot(final_df %>% filter(exp_val > 0), aes(x = exp_val, y = key)) +
  geom_boxplot(fill = "#55A868") +
  labs(title = "Exp. Value by Bookmaker",
       x = "Bookmaker",
       y = "Exp. Val") +
  theme_minimal()

### ----- MODEL EDGE -----------
## ---- MARKET ACCURACY --------
### ---- CALIBRATION PLOT (IMP. PROBS VS OUTCOMES) -----
calib_df <- final_df %>%
  select(team_name, game_id = id, adj_implied_prob, truth) %>%
  filter(!is.na(adj_implied_prob), !is.na(truth)) %>%
  # Bin Probabilities
  mutate(prob_bin = cut(adj_implied_prob,
                        breaks = seq(0, 1, by = 0.025),
                        include.lowest = TRUE)) %>%
  #Calculate actual win rates per bin
  group_by(prob_bin) %>%
  summarise(
    avg_pred = mean(adj_implied_prob),
    win_rate = mean(truth),
    count = n()
  ) %>%
  filter(count >= 5)  # optional: drop bins with few games

ggplot(calib_df, aes(x = avg_pred, y = win_rate)) +
  geom_point(size = 2, color = "#1f77b4") +
  geom_line(color = "#1f77b4") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray40") +
  labs(
    title = "Calibration of Market-Implied Probabilities",
    x = "Implied Probability (Market)",
    y = "Actual Win Rate"
  ) +
  theme_minimal()

calib_df <- final_df %>%
  select(team_name, game_id = id, prob_pred, truth) %>%
  filter(!is.na(prob_pred), !is.na(truth)) %>%
  # Bin Probabilities
  mutate(prob_bin = cut(prob_pred,
                        breaks = seq(0, 1, by = 0.025),
                        include.lowest = TRUE)) %>%
  #Calculate actual win rates per bin
  group_by(prob_bin) %>%
  summarise(
    avg_pred = mean(prob_pred),
    win_rate = mean(truth),
    count = n()
  ) %>%
  filter(count >= 5)  # optional: drop bins with few games

ggplot(calib_df, aes(x = avg_pred, y = win_rate)) +
  geom_point(size = 2, color = "#1f77b4") +
  geom_line(color = "#1f77b4") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray40") +
  labs(
    title = "Calibration of Prediction Probabilities",
    x = "Implied Probability",
    y = "Actual Win Rate"
  ) +
  theme_minimal()

# Brier Score = mean squared error between predicted prob and outcome
brier_score <- function(pred, actual) {
  mean((pred - actual)^2)
}
brier_raw <- brier_score(final_df$prob_pred, as.integer(final_df$truth))
# brier_adj <- brier_score(final_df$adj_implied_prob, final_df$actual_outcome)

# Log loss = - mean log-likelihood of actual outcomes given the predicted prob
log_loss <- function(pred, actual) {
  eps <- 1e-15  # to avoid log(0)
  pred <- pmin(pmax(pred, eps), 1 - eps) - mean(actual * log(pred) + (1 - actual) * log(1 - pred))
  }

# logloss_raw <- log_loss(final_df$prob_pred, as.integer(final_df$truth))
# logloss_adj <- log_loss(final_df$adj_implied_prob, final_df$actual_outcome)

cat("Brier Score (pred):", brier_raw, "\n")
cat("Brier Score (adj):", brier_adj, "\n")
# cat("Log Loss (pred):", logloss, "\n")
# cat("Log Loss (adj):", logloss_adj, "\n")

### ---- BIN ODDS RANGES
odds_binned <- final_df %>%
  filter(!is.na(price), !is.na(game_won)) %>%
  mutate(odds_bin = cut_number(price, n = 7,
                               dig.lab = 10 )) %>%
  group_by(odds_bin) %>%
  summarise(n = n(), win_rate = mean(game_won), groups = "drop") %>%
  arrange(desc(odds_bin))

ggplot(odds_binned, aes(x = odds_bin, y = win_rate)) +
  geom_col(fill = "#1f77b4") +
  geom_text(aes(label = n), vjust = -0.5, size = 3) +  # Adjust vjust as needed
  labs(
    title = "Win Rate by American Odds Bin",
    x = "Odds Bin",
    y = "Actual Win Rate"
  ) +
  theme_minimal()

### ---- HISTORICAL ROI BY ODDS BUCKETS -----
roi_by_bin <- final_df %>%
  mutate(odds_bin = cut_number(price, n = 7,
                               dig.lab = 10 )) %>%
  filter(model_pick) %>%
  mutate(odds_bin = cut_number(price, n = 7, dig.lab = 10),
         roi = if_else(game_won == 1,
                       if_else(price > 0, price / 100, 100 / abs(price))
                       ,1)) %>%
  group_by(odds_bin) %>%
  summarise(
    avg_roi = mean(roi, na.rm = TRUE),
    n = n(),
    .groups = "drop"
  )
ggplot(roi_by_bin, aes(x = odds_bin, y = avg_roi*100)) +
  geom_col(fill = "#1170AA") +
  geom_text(aes(label = n), vjust = -0.5, size = 3) +
  labs(
    title = "Model ROI by Odds Bin",
    x = "Odds Bin",
    y = "Average ROI (%)"
  ) +
  theme_minimal()

### ---- MODEL-MKT PROB. DIFFS DISTRIBUTION -----
ggplot(final_df, aes(x = prob_diff)) +
  geom_histogram(binwidth = 0.05, fill = "#1f77b4", color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  labs(
    title = "Distribution of Model-Market Probability Differences",
    x = "Model Probability – Market Probability",
    y = "Number of Observations"
  ) +
  theme_minimal()
df_correct <- final_df %>% 
  filter(truth == final_df$pred_class)
ggplot(df_correct, aes(x = prob_diff)) +
  geom_histogram(binwidth = 0.05, fill = "#1f77b4", color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  labs(
    title = "Distribution of Model-Market Probability Differences (Correct Predictions)",
    x = "Model Probability – Market Probability (Correct)",
    y = "Number of Observations"
  ) +
  theme_minimal()

### ---- MODEL-MKT PROB. DIFFS DISTRIBUTION BY BOOKMAKER-----
top_books <- final_df %>%
  count(key, sort = TRUE) %>%
  slice_head(n = 4) %>%
  pull(key)

ggplot(final_df, aes(x = prob_diff)) +
  geom_histogram(binwidth = 0.025, fill = "#1f77b4", color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  facet_wrap(~ key, ncol = 2) +  # 2 columns × 2 rows = 4 facets
  labs(title = "Model–Market Probability Differences by Bookmaker",
       x = "Model Prob – Market Prob",
       y = "Count") +
  theme_minimal() 

ggplot(df_correct, aes(x = prob_diff)) +
  geom_histogram(binwidth = 0.025, fill = "#1f77b4", color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  facet_wrap(~ key, ncol = 2) +  # 2 columns × 2 rows = 4 facets
  labs(title = "Model–Market Probability Differences by Bookmaker (Correct Predictions)",
       x = "Model Prob – Market Prob (Correct)",
       y = "Count") +
  theme_minimal() 

## ---- INSIGHT SUMMARY --------
### --- SUMMARY -------
### --- EXPLOITABLE ZONES (TEAM/ODDS TYPES) BY STRATEGY -------

# ---- BETTING STRATEGY & EXECUTION -------
## ---- FLAT BET STRATEGY ---- 
# Description: Implements a simple flat‑bet staking plan using
#              model probabilities vs vig‑free market probabilities.
#              * $100 stake per qualifying bet
#              * Initial bankroll = $10,000
#              * Bet placed if edge ≥ 0.03 AND expected value > 0
#              * Chooses bettor‑friendly American odds across books

library(dplyr)
library(lubridate)
library(ggplot2)


# Pick the best (most profitable) odds for the bettor across books
#  • For positive odds, take the highest number (bigger payout)
#  • For negative odds, take the number closest to 0 (least juice)
choose_best_odds <- function(odds_vec) {
  pos <- odds_vec[odds_vec > 0]
  neg <- odds_vec[odds_vec < 0]
  if (length(pos) > 0) {
    return(max(pos, na.rm = TRUE))
  } else {
    return(neg[which.min(abs(neg))])
  }
}

### ---- Strategy Parameters ----
edge_threshold   <- 0.03    # minimum edge to fire
stake_per_bet    <- 100     # flat stake size
initial_bankroll <- 10000   # starting bankroll

### ---- Data Requirements ----
#  bets_df should have ONE ROW PER TEAM per game with at least:
#   • game_id, game_datetime (POSIXct)
#   • team (factor / character)
#   • model_prob            ‑ model predicted win probability
#   • vig_free_prob         ‑ market implied prob w/ vig removed
#   • actual_outcome        ‑ 1 = win, 0 = loss (for back‑test)
#   • odds_* columns        ‑ American odds from one or more books
# Example load:  bets_df <- readRDS("joined_predictions_odds.rds")
### ---- Strategy Logic (FLAT BETS) ----
#### ---- 1. Prep Odds & Pick Best Line----
best_bets <- final_df %>% filter(model == "meta_model") %>%
  # arrange(pull_time) %>%
  select(c(names(final_df)[6:10],"game_won","game_id","venueLocation",
           "winning_team",names(final_df)[317:dim(final_df)[2]])) %>%
  # drop data flukes that occur in odds (eg. + 2200)
  filter(odds_id != "4a63cc547e2d0fe8f6c9d1efcdaff916")  %>%
  group_by(odds_id,) %>% 
  slice_max(dec_odds, n = 1, with_ties = FALSE) %>% 
  ungroup() %>%
  #grab only postive classifications (actual h2h bets)
  filter(pred_class == 1)
  # arrange(pull_time)

id_counts <- best_bets %>% count(game_id, name = "n")
unique(id_counts$n)

#### ---- 2. Apply flat-bet rules ------ 
flat_bets <- best_bets %>% 
  filter(model_edge >= vig_buffer, exp_val > 0) %>%
  mutate(stake  = stake_per_bet, payout = stake * (dec_odds - 1))   

#### ---- 3. settle bets & update bankroll -----
initial_bankroll <- 10000
bet_log <- flat_bets %>% 
  mutate( # time since previous bet, in hours (to control bankroll temporaly)
    hours_since_prev = as.numeric(difftime(startTimeUTC,
                                           lag(startTimeUTC)),
                                  units = "hours"),
    # start a new segment if it's the first bet or gap >= 4h
    new_segment = if_else(is.na(hours_since_prev) | hours_since_prev >= 4, 1, 0),
    segment_id = cumsum(new_segment)) %>%
  mutate(profit = if_else(game_won == 1, payout, -stake),
         bankroll = initial_bankroll + cumsum(profit)) %>%
  group_by(segment_id) %>% 
  mutate(
    # bankroll after the LAST bet of this segment, replicated to every row
    bankroll_segment = last(bankroll)
  ) %>% ungroup()
sum(bet_log$profit)
tail(bet_log$bankroll)

bet_log2 <- bet_log  %>%
  group_by(segment_id) %>% 
  mutate(
    # bankroll after the LAST bet of this segment, replicated to every row
    bankroll_segment = last(bankroll)) %>% ungroup()
sum(bet_log2$profit)
tail(bet_log2$bankroll)

### ----Strategy Logic (KELLY BETS) ----- 
# 1) Compute Kelly fraction (no dollar amounts yet)
kelly_prep <- best_bets %>%
  mutate( # time since previous bet, in hours (to control bankroll temporaly)
    hours_since_prev = as.numeric(difftime(startTimeUTC, lag(startTimeUTC)),
                                  units = "hours"),
    # start a new segment if it's the first bet or gap >= 4h
    new_segment = if_else(is.na(hours_since_prev) | hours_since_prev >= 4, 1, 0),
    segment_id = as.character(cumsum(new_segment))) %>%
  filter(model_edge >= vig_buffer,
         exp_val >= 0) %>%
  mutate(b = dec_odds - 1,
         raw_kelly = (prob_pred * b - (1 - prob_pred)) / b,
         kelly_frac = pmax(raw_kelly, 0)) %>%
  add_count(segment_id, name = "segment_length") 

kelly_log <- kelly_prep %>% mutate(stake = NA_real_, profit = NA_real_, 
                                   bankroll = NA_real_)
current_bankroll <- initial_bankroll            # closes *previous* segment
current_segment <- kelly_log$segment_id[1]     # track when we switch
segment_bankroll <- current_bankroll            # fixed stake-base for this segment

# 2) “For‐loop” the sequential Kelly stakes and bankroll
for (i in seq_len(nrow(kelly_log))) {
  #If entered a new segment, freeze a new base bankroll
  seg <- kelly_log$segment_id[i]
  edge <- abs(kelly_log$model_edge[i] - kelly_log$vig_buffer[i])
  seg_len <- kelly_log$segment_length[i]
  if (seg != current_segment) {
    print(paste0("New Bankroll: ", segment_bankroll))
    current_segment <- seg
    segment_bankroll <- current_bankroll        # bankroll at end of last segment
  }
  
  # size the stake off the _current_ bankroll / segment_bankroll
  s <- kelly_log$kelly_frac[i] * segment_bankroll *  pmin(.1, (1/seg_len))
  # settle profit
  win <- kelly_log$game_won[i] == 1
  p <- if (win) s * kelly_log$b[i] else -s
  # update the book
  current_bankroll <- current_bankroll + p
  #Write results back to log
  kelly_log$stake[i] <- s
  kelly_log$profit[i] <- p
  kelly_log$bankroll[i] <- current_bankroll
}
sum(kelly_log$profit)
tail(kelly_log$bankroll)


kelly_log2 <- kelly_log  %>%
  group_by(segment_id) %>% 
  mutate(
    # bankroll after the LAST bet of this segment, replicated to every row
    bankroll_segment = last(bankroll)) %>% ungroup()
sum(kelly_log2$profit)
tail(kelly_log2$bankroll)

# Inspect
kelly_log %>%
  select(timestamp, pred_prob, dec_odds, kelly_frac, stake, profit, bankroll)

sum(kelly_log2$profit)
tail(kelly_log2$bankroll)

# Inspect the running bankroll curve
bet_log %>% 
  ggplot(aes(row_number(), bankroll)) +
  geom_line() +
  labs(x = "Bet #", y = "Bankroll ($)", title = "Flat-bet strategy bankroll")