# ---- LOAD LIBRARIES ----------------------------------------------------------------------
library(tidyverse)
library(lubridate)
library(tidymodels)

# ---- PATHS AND FILES ---------------------------------------------------------------------
rds_files_path <- getwd()
combined_log_path <- file.path(rds_files_path, "Data/team_unplayed_combined_predictions.csv")
combined_log <- read.csv(combined_log_path)
latest_scores <- combined_log %>%
  filter(!is.na(prediction_time)) %>% 
  filter(prediction_time <= startTimeUTC_curr) %>%
  group_by(model_version, game_id, teamId) %>%
  slice_max(order_by = prediction_time, n = 1, with_ties = FALSE) %>%
  ungroup()

unique(latest_scores$model_version)

# 1) Filter to completed games and add y, y_hat:
df <- latest_scores %>% filter(!is.na(actual_outcome)) %>%
  mutate(
    y      = as.integer(actual_outcome),
    y_hat  = as.integer(pred_class),         # ensure 0/1
    p_hat  = pred_probability
  )
# 1.a) Build a count table of (model_version, actual, predicted):
conf_df <- df %>%
  count(model_version, y, y_hat) %>%
  complete(
    model_version,
    y     = c(0, 1),
    y_hat = c(0, 1),
    fill = list(n = 0)
  ) %>%
  mutate(
    Actual    = factor(y,   levels = c(0,1), labels = c("True 0","True 1")),
    Predicted = factor(y_hat, levels = c(0,1), labels = c("Pred 0","Pred 1"))
  )

# 1.b) Plot a faceted confusion matrix (counts) per model_version:
ggplot(conf_df, aes(x = Predicted, y = Actual, fill = n)) +
  geom_tile(color = "white") +
  geom_text(aes(label = n), size = 5) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  facet_wrap(~ model_version) +
  labs(
    x = "Predicted", 
    y = "Actual",
    title = "Confusion Matrix by Model Version"
  ) +
  theme_minimal()

models <- unique(latest_scores$model_version) #Extract all unique model_version values:
# 2) Define a helper to compute metrics for one model
compute_metrics <- function(sub_df) {
  N  <- nrow(sub_df)
  TP <- sum(sub_df$y == 1 & sub_df$y_hat == 1)
  TN <- sum(sub_df$y == 0 & sub_df$y_hat == 0)
  FP <- sum(sub_df$y == 0 & sub_df$y_hat == 1)
  FN <- sum(sub_df$y == 1 & sub_df$y_hat == 0)
  
  # Accuracy
  accuracy <- (TP + TN) / N
  
  # Sensitivity and Specificity
  sensitivity <- if ((TP + FN) > 0) TP / (TP + FN) else NA_real_
  specificity <- if ((TN + FP) > 0) TN / (TN + FP) else NA_real_
  
  # Brier Score
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
    ROC_AUC     = roc_auc
  )
}

# 4) Loop over each model_version and bind results:
results_list <- lapply(models, function(m) {
  df_sub <- filter(df, model_version == m)
  metrics <- compute_metrics(df_sub)
  bind_cols(model_version = m, metrics)
})

metrics_by_model <- bind_rows(results_list)

# 5) Inspect:
metrics_by_model
