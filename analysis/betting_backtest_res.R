library(tidyverse)
library(tidyr)
library(lubridate)
library(tidymodels)
library(ggplot2)
library(stringr)
library(purrr)
library(probably)
library(betacal)
library(themis)
library(ranger)
library(finetune)

f_path <- getwd()
# rf_rolling_grid <- rf_res_rolling_bayes
# team_splits <- names(readRDS(paste0(f_path, "/Data/team_splits_v2.rds")))
team_df_played <- readRDS(paste0(f_path, "/Data/team_df_played_v3.rds"))
rf_rolling_grid <- readRDS(paste0(f_path, "/Data/team_rocv_res_rf_fit_v3.rds"))
rf_final_fit <- readRDS(paste0(f_path, "/Data/team_rocv_final_rf_fit_v3.rds"))
rf_rolling_wf <- readRDS(paste0(f_path, "/Data/team_wf_rf_v3.rds"))
team_df <- readRDS(file.path(f_path, "Data/team_df_v2.rds"))
gc()

best_rf_rolling <- select_best(rf_rolling_grid, metric = "roc_auc")
best_mtry <- best_rf_rolling %>% pull(mtry)
best_min_n <- best_rf_rolling %>% pull(min_n)
team_metrics <- rf_rolling_grid %>% collect_metrics(summarize = FALSE)
team_metrics_best <- team_metrics %>% 
  filter(mtry == best_mtry, min_n == best_min_n)
team_metrics_avg <- team_metrics_best %>%
  group_by(.metric) %>%
  summarize(mean_est = mean(.estimate, na.rm = TRUE), .groups = "drop")
team_metrics_avg


m_long <- rf_rolling_grid %>% collect_metrics(summarize = FALSE)%>% 
  filter(.metric == "roc_auc")  # keep only ROC-AUC
m_long <- m_long %>% mutate(slice_num = as.integer(str_extract(id, "\\d+")))

t_long <- rf_final_fit %>% collect_metrics(summarize = FALSE) %>% 
  filter(.metric == "roc_auc")            # keep only ROC-AUC

### --- UPDATED (USE THIS!!!) -----
all_preds <- rf_rolling_grid %>% 
  collect_predictions() %>% 
  mutate(slice_num = as.integer(str_extract(id, "\\d+"))) 
test_preds <- rf_final_fit %>% 
  collect_predictions()

get_best_up_to <- function(k) {
  m_long %>%
    filter(slice_num < k) %>%
    group_by(mtry, min_n,trees) %>%
    summarise(mean_auc = mean(.estimate), .groups = "drop") %>%
    slice_max(mean_auc, n = 1) %>%
    # slice_min(trees, n = 1) %>%
    mutate(slice_num = k)
}
best_param_log <- map_dfr(sort(unique(m_long$slice_num)), get_best_up_to)

backtest_preds <- best_param_log %>%
  inner_join(
    all_preds,
    by = c("slice_num","mtry", "min_n","trees")) %>%
  mutate(game_won = factor(game_won, levels = c(1, 0)))

#Sanity Checks
assess1 <- assessment(rf_rolling_grid$splits[[1]])
assess_all <- map_dfr(rf_rolling_grid$splits[-1], assessment)
length(unique(assess_all$game_id[duplicated(assess_all$game_id)]))
id_counts <- assess_all %>% count(game_id, name = "n")
unique(id_counts$n)
# rm(assess_all)

test_datas <- 0 
for (i in 2:length(rf_rolling_grid$id)) {
  print(dim(assessment(rf_rolling_grid$splits[[i]])))
  test_datas <- test_datas + dim(assessment(rf_rolling_grid$splits[[i]]))[[1]]
}

#Map hockey data to prediction data
# Create a named list of assessment sets using the correct best config per slice
assess_all <- map_dfr(
  best_param_log$slice_num,
  function(s) {
    best_row <- best_param_log %>% filter(slice_num == s)
    # Subset predictions from the full object
    assessment_data <- rf_rolling_grid %>%
      collect_predictions() %>%
      mutate(slice_num = as.integer(str_extract(id, "\\d+"))) %>%
      filter(slice_num == s,
             mtry == best_row$mtry,
             min_n == best_row$min_n,
             trees == best_row$trees #,.iter == best_row$.iter
             )  
    # Attach assessment data for game_id matching (or custom join later)
    split_data <- assessment(rf_rolling_grid$splits[[s]])
    # Return the raw assessment data (to be joined later with preds)
    split_data %>% 
      mutate(slice_num = s) %>% 
      # select(slice_num,game_id,teamId,startTimeUTC) %>% 
      select(-slice_num)
  })
# assess_all_t <- assess_all %>%
#   left_join(team_df %>% select(game_id, teamId, startTimeUTC) %>%
#               rename(startTimeUTC_curr = startTimeUTC),
#             by = c("game_id", "teamId"))

#Sanity Checks
length(unique(assess_all$game_id[duplicated(assess_all$game_id)]))
id_counts <- assess_all %>% count(game_id, name = "n")
unique(id_counts$n)
train_preds <- bind_cols(backtest_preds, assess_all)

#Rename game_won... and remove other game_won....
sum(train_preds$game_won...11 == train_preds$game_won...301) == nrow(backtest_preds)
sum(backtest_preds$game_won == assess_all$game_won) == nrow(backtest_preds)
train_preds <- train_preds %>%
  rename(game_won = game_won...11) %>%
  select(-game_won...301)

#Add Test Split into data
##1. Determine what the “next” slice number is
max_slice <- max(train_preds$slice_num, na.rm = TRUE)
new_slice <- max_slice + 1

## 2. Pull out the best hyperparameters for that final slice
##    (i.e. best over all slices < new_slice)
best_final <- get_best_up_to(new_slice) 

## 3. Extract the final‐split predictions
final_preds <- rf_final_fit %>% 
  collect_predictions() %>% 
  # mirror backtest_preds structure
  mutate(
    slice_num = new_slice,
    mtry      = best_final$mtry,
    min_n     = best_final$min_n,
    trees     = best_final$trees,
    mean_auc  = best_final$mean_auc 
    # .iter     = best_final$.iter, # if you care about .iter, otherwise drop this line
  )
assess_test <- map_dfr(rf_final_fit$splits[1], assessment)
test_preds <- bind_cols(assess_test, final_preds)
#Sanity Checks
sum(test_preds$game_won...289 == test_preds$game_won...312) == nrow(test_preds)
sum(final_preds$game_won == assess_test$game_won) == nrow(test_preds)

#Rename game_won... and remove other game_won....
test_preds <- test_preds %>%
  rename(game_won = game_won...289) %>%
  select(-game_won...312)

##4. Row‐bind to your combined_preds
combined_preds <- bind_rows(train_preds,test_preds) %>%
  mutate(pred_probability = .pred_1, pred_class = .pred_class)

## ----------------------------------------------------------
## 2.  Rolling beta-calibration:
##     – fit on slices < k
##     – apply to slice  k
## ----------------------------------------------------------
cal_start <- 3 #Select last slices for debugging     # first slice to calibrate
slice_vec <- sort(unique(combined_preds$slice_num))

calibrated_preds <- map_dfr(
  slice_vec[slice_vec >= cal_start],
  function(k) {
    # --- 2a. Fit calibrator on *past* predictions -------------
    past <- combined_preds %>% filter(slice_num < k) %>% select(-c(.config,.row))
    beta_mod <- cal_estimate_beta(past, truth = game_won, estimate = .pred_1)
    # --- 2b. Apply calibrator to current slice ---------------
    combined_preds %>% 
      filter(slice_num == k) %>% 
      cal_apply(beta_mod) %>%               # replaces .pred_1 with calibrated vals
      mutate(cal_class = if_else(.pred_1 >= 0.50, 1, 0), # optional hard class
             cal_probability = .pred_1) %>%
      filter(slice_num == k)
  }
)

#Check Calibration on Sensitive data
# 1. sensitivity before calibration (should match tuning)
pre_cal <- calibrated_preds %>%
  # filter(slice_num >= start_idx) %>%
  mutate(y      = as.numeric(as.character(game_won)),
         y_hat0 = as.integer(pred_class == 1))
sens_pre <- pre_cal %>% 
  filter(y == 1) %>%                 # actual wins
  summarise(sens = mean(y_hat0)) %>% 
  pull(sens)
# 2. sensitivity after calibration (your reported 0.551)
post_cal <- calibrated_preds %>% 
  mutate(
    y      = as.numeric(as.character(game_won)),
    y_hat  = as.integer(cal_class),         # ensure 1/0
    p_hat  = as.numeric(cal_probability))
sens_post <- post_cal %>% 
  filter(y == 1) %>%                 # actual wins
  summarise(sens = mean(y_hat)) %>% 
  pull(sens)


#Get most recent split after the 15th game of the 2025 season
#renact the backtest starting from games where all rolling variables are populated correctly

team_16th <- team_df_played %>%   
  filter(season == "2024",                # regular season only
         gameType == 2) %>%               # if you store gameType (2 = RS)
  arrange(teamId, game_date) %>%          # chronological within team
  group_by(teamId) %>% 
  mutate(game_num = row_number()) %>%     # 1, 2, … per team
  ungroup() %>% 
  filter(game_num == 16) %>%              # the 15th game
  select(teamId, game_id, game_date,is_home, opp_teamId,game_num)

max_16th_game_id <- team_16th %>%             # each team’s 16th game
  filter(game_date == min(game_date)) %>% # on the min 16th-game date
  distinct(game_id) %>%                  # one row per game
  slice_head(n = 1) %>%                  # pick the first
  pull(game_id)

# Find which split's assessment set contains game 16 of 2024–25
target_season <- "2024"
split_index <- combined_preds %>%
  filter(season  == target_season,
         game_id == max_16th_game_id) %>%
  pull(slice_num) %>%
  unique()

# backtest start
start_idx <- split_index
end_idx   <- max(combined_preds$slice_num)
gc()

#Select tested data from split onwards
bettable_2024 <- calibrated_preds %>%
  filter(slice_num >= start_idx) %>% filter(gameType == 2)

#Check Non-unique game_ids (make sure split works)
length(unique(bettable_2024$game_id[duplicated(bettable_2024$game_id)]))
length(unique(bettable_2024$game_id))*2 == (dim(bettable_2024)[1])
id_counts <- bettable_2024 %>% count(game_id, name = "n")
unique(id_counts$n)

#Save
saveRDS(bettable_2024, paste0(f_path,"/Data/rf_bettable_20242025.rds"))
rm(all_preds, combined_preds, calibrated_preds, rf_rolling_grid, 
   rf_rolling_wf,rf_final_fit,assess_all, backtest_preds, best_final,
   train_preds, test_preds, id_counts, m_long, t_long, assess_test, 
   assess1, best_param_log)
gc()

#LOAD AND TRANSFORM DATA (ODDS & BETTABLE SEASON)
#Convert Time Stamps
bettable_2024 <- readRDS(paste0(f_path,"/Data/rf_bettable_20242025.rds"))
bettable_2024 <- bettable_2024  %>%
  left_join(team_df %>% select(game_id, teamId, startTimeUTC) %>%
              rename(startTimeUTC_curr = startTimeUTC),
            by = c("game_id", "teamId")) %>%
  rename(bet_game_id = id) %>%
  mutate(cal_class = ifelse(cal_probability >= .50, 1, 0),
         startTimeUTC_curr = as.POSIXct(startTimeUTC_curr, 
                                        format = "%Y-%m-%dT%H:%M:%SZ",
                                        tz = "UTC"), # "2025-04-01T00:30:00Z"
         startTimeUTC = as.POSIXct(startTimeUTC, format = "%Y-%m-%d %H:%M:%S",
                                   tz = "UTC"), # "2025-04-01 00:30:00"
         game_date = as.Date(game_date),
         model_date = as.Date(startTimeUTC),
         home_team = paste0(home_team_locale, " ",home_team_name),
         away_team = paste0(away_team_locale, " ",away_team_name),
         team_name = case_when(
           teamId == home_id ~ paste0(home_team_locale, " ",home_team_name),
           teamId == away_id ~ paste0(away_team_locale, " ",away_team_name))
         ) %>%
  mutate(
    home_team = case_when(
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
      TRUE ~ team_name)) %>% ungroup()

# 1) Filter to completed games and add y, y_hat:
df <- bettable_2024 %>% filter(!is.na(game_won)) %>%
  mutate(
    y      = as.numeric(as.character(game_won)),
    y_hat  = as.integer(cal_class),         # ensure 0/1
    p_hat  = as.numeric(cal_probability))

# 1.a) Build a count table of (actual, predicted):
conf_df <- df %>%
  count(y, y_hat)%>%
  mutate(
    Actual    = factor(y,   levels = c(1,0), labels = c("True 1","True 0")),
    Predicted = factor(y_hat, levels = c(1,0), labels = c("Pred 1","Pred 0")))

# 1.b) Plot a faceted confusion matrix (counts) per model_version:
ggplot(conf_df, aes(x = Predicted, y = Actual, fill = n)) +
  geom_tile(color = "white") +
  geom_text(aes(label = n), size = 5) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  labs(
    x = "Predicted", 
    y = "Actual",
    title = "Confusion Matrix by Model Version"
  ) +
  theme_minimal()

# ggplot(conf_df, aes(x = Predicted, y = Actual, fill = n)) +
#   geom_tile(color = "white") +
#   geom_text(aes(label = n), size = 5) +
#   scale_fill_gradient(low = "white", high = "steelblue") +
#   labs(
#     x = "Predicted",
#     y = "Actual",
#     title = "Confusion Matrix by Model Version"
#   ) +
#   theme_minimal()

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
results_list <- compute_metrics(df)
# 5) Inspect:
results_list

## ---- ODDS DATA ----------------------------------------------------------------------
f_path <- getwd()
odds <- readRDS(paste0(f_path, "/Data/odds_data_20242025.rds"))
offtime_odds <- odds %>% filter(commence_time <= timestamp)
#Check timestamps are before games
nrow(odds) - nrow(offtime_odds) 

odds <- odds %>%
  group_by(id, markets_key, key, title, markets_last_update, timestamp) %>%
  filter(commence_time > timestamp,
         markets_key == "h2h",
         n() == 2) %>%  # Only keep games with exactly 2 rows (one per team)
  arrange(desc(implied_prob)) %>%
  mutate(side = c("fav", "dog")) %>%
  ungroup() %>%
  pivot_wider(
    id_cols = c(id, markets_key, key, title, markets_last_update,timestamp, 
                commence_time, away_team, home_team,last_update),
    names_from = side,
    values_from = c(name, price, implied_prob),
    names_glue = "{.value}_{side}") %>%
  #Calcualte Dec Odds & VIG Adj
  mutate(adj_implied_prob_fav = implied_prob_fav / (implied_prob_fav + implied_prob_dog),
         adj_implied_prob_dog = implied_prob_dog / (implied_prob_fav + implied_prob_dog),
         vig = (implied_prob_fav + implied_prob_dog - 1)
  ) %>%
  pivot_longer(
    cols = c(name_fav, name_dog, price_fav, price_dog, implied_prob_fav, 
             implied_prob_dog, adj_implied_prob_fav, adj_implied_prob_dog),
    names_to = c(".value", "side"),
    names_pattern = "^(.*)_(fav|dog)$") %>%
  group_by(id, markets_key, timestamp,last_update) %>%
  mutate(vig_book_spread = max(vig) - min(vig),
         min_book_prob = min(adj_implied_prob, na.rm = TRUE),
         max_book_prob = max(adj_implied_prob, na.rm = TRUE)) %>% ungroup() %>%
  mutate(
    commence_time = as.POSIXct(commence_time, format = "%Y-%m-%dT%H:%M:%SZ",
                               tz = "UTC"), # "2025-03-31T19:25:18Z"s
    timestamp = as.POSIXct(timestamp, format = "%Y-%m-%dT%H:%M:%SZ",
                           tz = "UTC"), # "2025-04-01T00:30:00Z"
    odds_date = as.Date(commence_time),
    dec_odds = if_else(price > 0, price / 100 + 1, # positive American odds
                       100 / abs(price) + 1 # negative American odds
                       ))


latest_odds <- odds %>%
  filter(!is.na(markets_last_update)) %>% 
  filter((markets_last_update < commence_time)) %>%
  group_by(id,key,name,last_update) %>%
  slice_max(order_by = last_update, n = 1, with_ties = FALSE) %>%
  ungroup()

## ---- JOIN MODEL LOG AND ODDS ---------
#Check to see team names are the same
setdiff(unique(bettable_2024$home_team), unique(odds$home_team))
setdiff(unique(bettable_2024$away_team), unique(odds$away_team))
setdiff(unique(bettable_2024$team_name), unique(odds$name))
sum(is.na(latest_odds$price))

#Create a joined dataframe
joined1 <- bettable_2024 %>%
  left_join(latest_odds, by = c("home_team", "away_team", "team_name" = "name",
                                "startTimeUTC" = "commence_time",
                                "game_date" = "odds_date")) 
unmatched1 <- joined1 %>% #Extract rows still unmatched after stage 1
  filter(is.na(price)) %>%
  select(names(bettable_2024))

joined2 <- unmatched1 %>%
  left_join(latest_odds, by = c("home_team", "away_team", "team_name" = "name",
                                "game_date" = "odds_date"))
unmatched2 <- joined2 %>% #still unmatched after stage 2
  filter(is.na(price)) %>%
  select(names(bettable_2024))

# joined3 <- unmatched2 %>%
#   left_join(latest_odds, by = c("home_team", "away_team", "team_name" = "name",
#                                 "model_date" = "odds_date"))
# unmatched3 <- joined3 %>% #still unmatched after stage 2
#   filter(is.na(price)) %>%
#   select(names(bettable_2024))


# 8. Combine:
matched1 <- joined1 %>% filter(!is.na(price))
# dim(matched1)
# sum(is.na(matched1$commence_time))
matched2 <- joined2 %>% filter(!is.na(price))
# dim(matched2)
# sum(is.na(matched2$commence_time))

final_df <- matched1 %>% # bind_rows(matched1, matched2)%>% 
  mutate(model_edge = cal_probability - adj_implied_prob,
         vig_buffer = implied_prob - adj_implied_prob,
         exp_val = (cal_probability * dec_odds) - 1,
         model_pick = exp_val > 0,
         prob_diff = cal_probability - adj_implied_prob) %>%
  arrange(id,key,timestamp,game_time)

#Sanity Checks
id_counts <- final_df %>% count(game_id, name = "n")
unique(id_counts$n)
test_df <- final_df %>%
  left_join(team_df %>% 
              select(game_id, teamId, game_won, startTimeUTC) %>%
              rename(startTimeUTC_curr = startTimeUTC),
            by = c("game_id", "teamId"))
sum(test_df$game_won.x == test_df$game_won.y)
sum(test_df$startTimeUTC_curr == final_df$startTimeUTC_curr)

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
stake_per_bet    <- 100    # flat stake size
initial_bankroll <- 1000  # starting bankroll

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
best_bets <- final_df %>%
  select(c(names(final_df)[6:10],"game_won","game_id","venueLocation",
           "winning_team",names(final_df)[317:dim(final_df)[2]])) %>%
  arrange(startTimeUTC_curr,id,key) %>%
  # drop data flukes that occur in odds (eg. + 2200)
  filter(id != "4a63cc547e2d0fe8f6c9d1efcdaff916") %>% group_by(id) %>% 
  slice_max(dec_odds, n = 1, with_ties = FALSE) %>% ungroup() %>%
  arrange(startTimeUTC_curr,id,key) %>%
  #grab only postive classifications (actual h2h bets)
  filter(cal_class == 1)


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
    hours_since_prev = as.numeric(difftime(startTimeUTC_curr,
                                           lag(startTimeUTC_curr)),
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
    hours_since_prev = as.numeric(difftime(startTimeUTC_curr, lag(startTimeUTC_curr)),
                                  units = "hours"),
    # start a new segment if it's the first bet or gap >= 4h
    new_segment = if_else(is.na(hours_since_prev) | hours_since_prev >= 4, 1, 0),
    segment_id = as.character(cumsum(new_segment))) %>%
  filter(model_edge >= vig_buffer,
         exp_val > 0) %>%
  mutate(b = dec_odds - 1,
         raw_kelly = (cal_probability * b - (1 - cal_probability)) / b,
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
  s <- kelly_log$kelly_frac[i] * segment_bankroll *  pmin(.15,(1/seg_len))
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
