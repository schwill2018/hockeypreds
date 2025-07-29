library(dplyr)
library(purrr)
library(quickcode)
library(tidyverse)
rds_files_path <- getwd()
team_df <- readRDS(paste0(rds_files_path, "/Data/team_df.rds"))

## 0.  Base game‑table (one home row per game) --------------------------------
base_games <- team_df %>%
  filter(is_home == 1) %>%
  select(game_id, game_date, season, home_id, away_id,
         home_score, away_score, startTimeUTC,
         team_CF, team_CA,
         team_powerPlayGoals, team_powerPlayGoals_opp) %>%
  arrange(startTimeUTC, game_id) %>%
  mutate(
    goal_diff = home_score - away_score,
    corsi_diff = team_CF - team_CA,
    pp_goal_diff = team_powerPlayGoals - team_powerPlayGoals_opp)

## 1.  Generic Elo engine -----------------------------------------------------
elo_run <- function(games, ratings = list(), K, scale = 400, 
                    start_rating = 1500, outcome_vec, 
                    weight_vec   = rep(1, nrow(games))) {
  n <- nrow(games)
  elo_home_pre <- numeric(n)
  elo_away_pre <- numeric(n)
  p_home <- numeric(n)
  outcome_num <- as.numeric(outcome_vec)  # 1 / 0 indicator
  
  for (i in seq_len(n)) {
    g <- games[i, ]
    hid <- as.character(g$home_id)
    aid <- as.character(g$away_id)
    Ra <- ratings[[hid]] %or% start_rating
    Rb <- ratings[[aid]] %or% start_rating
    elo_home_pre[i] <- Ra
    elo_away_pre[i] <- Rb
    p <- 1 / (1 + 10 ^ (-(Ra - Rb) / scale))
    p_home[i] <- p
    
    delta <-  weight_vec[i] * K * (outcome_num[i] - p)
    ratings[[hid]] <- Ra + delta
    ratings[[aid]] <- Rb - delta
  }
  games_out <- games %>%
    mutate(elo_home_pre = elo_home_pre,
           elo_away_pre = elo_away_pre,
           p_home = p_home)
  list(games = games_out, ratings = ratings)
}

## 2.  Carry‑over helper ------------------------------------------------------
carry_over <- function(ratings, carry = 0.7) {
  if (length(ratings) == 0) return(ratings)
  mu <- mean(unlist(ratings))
  lapply(ratings, function(r) carry * r + (1 - carry) * mu)
}

## 3.  Tune K once on 2012‑14 (Win‑loss with margin weight) -------------------
wl_weight   <- log1p(abs(base_games$goal_diff))
wl_outcome  <- as.integer(base_games$goal_diff > 0)  # 1 if home wins

logloss <- function(p, y) {
  eps <- 1e-12
  p <- pmin(pmax(p, eps), 1 - eps)
  -(y * log(p) + (1 - y) * log1p(-p))
}

tune_K <- function(K_grid) {
  best <- Inf; K_opt <- NA; ratings <- list()
  for (K in K_grid) {
    ratings_k <- ratings; ll <- c()
    for (yr in 2012:2014) {
      rows <- base_games$season == yr
      res <- elo_run(base_games[rows, ], ratings_k, K,
                      outcome_vec = wl_outcome[rows],
                      weight_vec  = wl_weight[rows])
      ll <- c(ll, logloss(res$games$p_home, wl_outcome[rows]))
      ratings_k <- carry_over(res$ratings)
    }
    m <- mean(ll)
    if (m < best) { best <- m; K_opt <- K }
  }
  K_opt
}

K_best <- tune_K(8:40)

## 4.  Win‑loss Elo (margin weighted) 2015‑16 ---------------------------------
ratings <- list(); out_wl <- list()
for (yr in 2015:max(base_games$season)) {
  rows <- base_games$season == yr
  res <- elo_run(base_games[rows, ], ratings, K_best,
                  outcome_vec = wl_outcome[rows],
                  weight_vec  = wl_weight[rows])
  out_wl[[as.character(yr)]] <- res$games
  ratings <- carry_over(res$ratings)
}
wl_elo_games <- bind_rows(out_wl) %>%
  select(game_id, p_home_wl = p_home, elo_wl_home = elo_home_pre, 
         elo_wl_away = elo_away_pre)

## 5.  Possession Elo (Corsi diff, unweighted) --------------------------------
poss_outcome <- as.integer(base_games$corsi_diff > 0)
ratings <- list(); out_poss <- list()
for (yr in 2015:max(base_games$season)) {
  rows <- base_games$season == yr
  res  <- elo_run(base_games[rows, ], ratings, K_best,
                  outcome_vec = poss_outcome[rows])
  out_poss[[as.character(yr)]] <- res$games
  ratings <- carry_over(res$ratings)
}
poss_elo_games <- bind_rows(out_poss) %>%
  select(game_id, p_home_poss = p_home, elo_poss_home = elo_home_pre, 
         elo_poss_away = elo_away_pre)

## 6.  Power‑play Goals Elo (PPG diff, unweighted) -----------------------------
pp_outcome <- as.integer(base_games$pp_goal_diff > 0)
ratings <- list(); out_pp <- list()
for (yr in 2015:max(base_games$season)) {
  rows <- base_games$season == yr
  res <- elo_run(base_games[rows, ], ratings, K_best,
                  outcome_vec = pp_outcome[rows])
  out_pp[[as.character(yr)]] <- res$games
  ratings <- carry_over(res$ratings)
}
pp_elo_games <- bind_rows(out_pp) %>%
  select(game_id, p_home_pp = p_home, elo_pp_home = elo_home_pre, 
         elo_pp_away = elo_away_pre)

## 7.  Join all Elo features back to team_df ----------------------------------
team_df2 <- team_df %>% 
  filter(is_home == 1)%>%
  filter(season >= 2015) %>%
  left_join(wl_elo_games, by = "game_id") %>% 
  left_join(poss_elo_games, by = "game_id") %>% 
  left_join(pp_elo_games, by = "game_id") %>% 
  mutate(
    # own Elo pre-rating
    elo_wl_pre   = if_else(is_home == 1, elo_wl_home,   elo_wl_away),
    elo_poss_pre = if_else(is_home == 1, elo_poss_home, elo_poss_away),
    elo_pp_pre   = if_else(is_home == 1, elo_pp_home,   elo_pp_away),
    
    # win-probability for THIS team
    elo_wl_prob   = if_else(is_home == 1, p_home_wl,   1 - p_home_wl),
    elo_poss_prob = if_else(is_home == 1, p_home_poss, 1 - p_home_poss),
    elo_pp_prob   = if_else(is_home == 1, p_home_pp,   1 - p_home_pp),
    
    # hard class @ 0.50
    elo_wl_class   = as.integer(if_else(elo_wl_prob >= 0.5, 1, 0)),
    elo_poss_class = as.integer(if_else(elo_poss_prob >= 0.5, 1, 0)),
    elo_pp_class   = as.integer(if_else(elo_pp_prob >= 0.5, 1, 0))
  )

# spot the seasons that have NAs in any Elo column
# unique(team_df$season[ is.na(team_df$p_home_wl) ])
# --- 2.  Helper to compute metrics -------------------------------------------
compute_metrics <- function(df) {
  N  <- nrow(df)
  TP <- sum(df$y == 1 & df$y_hat == 1)
  TN <- sum(df$y == 0 & df$y_hat == 0)
  FP <- sum(df$y == 0 & df$y_hat == 1)
  FN <- sum(df$y == 1 & df$y_hat == 0)
  
  accuracy    <- (TP + TN) / N
  sensitivity <- if ((TP + FN) > 0) TP / (TP + FN) else NA_real_
  specificity <- if ((TN + FP) > 0) TN / (TN + FP) else NA_real_
  precision   <- if ((TP + FP) > 0) TP / (TP + FP) else NA_real_
  brier       <- mean((df$p_hat - df$y)^2)
  
  # Cohen’s κ
  P_yes_true <- (TP + FN) / N; P_no_true  <- (TN + FP) / N
  P_yes_pred <- (TP + FP) / N; P_no_pred  <- (TN + FN) / N
  P_o <- (TP + TN) / N; P_e <- P_yes_true * P_yes_pred + P_no_true * P_no_pred
  kappa <- if ((1 - P_e) > 0) (P_o - P_e) / (1 - P_e) else NA_real_
  
  # ROC AUC (pair-wise)
  pos_idx <- which(df$y == 1); neg_idx <- which(df$y == 0)
  wins <- ties <- 0
  for (i in pos_idx) for (j in neg_idx) {
    if      (df$p_hat[i] >  df$p_hat[j]) wins <- wins + 1
    else if (df$p_hat[i] == df$p_hat[j]) ties <- ties + 1
  }
  num_pairs <- length(pos_idx) * length(neg_idx)
  roc_auc <- if (num_pairs > 0) (wins + 0.5 * ties) / num_pairs else NA_real_
  
  tibble(Accuracy = accuracy, Sensitivity = sensitivity, Specificity = specificity,
         Precision = precision, Brier = brier, Kappa = kappa, ROC_AUC = roc_auc)
}

# --- 3.  Evaluate all Elo variants -------------------------------------------
model_tags <- c("wl", "poss", "pp")

results <- purrr::map_dfr(model_tags, function(tag) {
  df <- team_df2 %>% 
    filter(!is.na(game_won)) %>% 
    transmute(
      y = as.numeric(as.character(game_won)),
      p_hat = as.numeric(.data[[paste0("elo_", tag, "_prob")]]),
      y_hat = as.integer(.data[[paste0("elo_", tag, "_class")]])
    )
  compute_metrics(df) %>% mutate(model = tag)
})

print(results)

# --- 4.  (Optional) faceted confusion matrices -------------------------------
library(ggplot2)

conf_df <- team_df2 %>% 
  filter(!is.na(game_won)) %>% 
  mutate(y = as.integer(game_won)) %>% 
  pivot_longer(cols = ends_with("_class") & starts_with("elo_"),
               names_to = "model", values_to = "y_hat") %>% 
  count(model, y, y_hat) %>% 
  mutate(
    Actual = factor(y, levels = c(1,0), labels = c("True 1","True 0")),
    Predicted = factor(y_hat, levels = c(1,0), labels = c("Pred 1","Pred 0")),
    model = recode(model,
                   elo_wl_class  = "Win-loss Elo",
                   elo_poss_class = "Possession Elo",
                   elo_pp_class = "PP-Goal Elo"))

ggplot(conf_df, aes(Predicted, Actual, fill = n)) +
  geom_tile(colour = "white") +
  geom_text(aes(label = n), size = 4) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  facet_wrap(~ model) +
  labs(title = "Confusion matrices for three Elo variants") +
  theme_minimal()
