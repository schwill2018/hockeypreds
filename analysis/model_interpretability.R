library(tidymodels)
library(vip)
library(dplyr)
library(tidyr)
library(ggplot2)
library(tidyverse)
library(iml)
library(future)
library(future.callr)
library(xgboost)
options(future.globals.maxSize = 4 * 1024^3) #Max Ram 3 GB
rds_files_path <- getwd()

#–– 1) Fit your workflows on the training data ––
team_df_played <- readRDS(paste0(rds_files_path, "/Data/team_df_played_v3.rds"))
team_recipe <- readRDS(paste0(rds_files_path, "/Data/team_recipe_goal_v3.rds"))
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
rf_fit <- readRDS(paste0(rds_files_path, "/Data/team_deployable_model_rf.rds"))
final_rf_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_rf.rds"))
log_fit <- readRDS(paste0(rds_files_path, "/Data/team_rocv_res_glm_fit_v3.rds"))
nn_fit <- readRDS(paste0(rds_files_path, "/Data/team_deployable_model_mlp.rds"))
final_xgb_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_xgb_v3.rds"))
xgb_fit <- readRDS(paste0(rds_files_path, "/Data/team_rocv_final_xgb_fit_v3.rds")) #readRDS(paste0(rds_files_path, "/Data/team_deployable_model_xgb.rds"))
gc()


#–– 2) Logistic Regression: Coefficients as importance ––
log_coefs <- 
  tidy(extract_fit_parsnip(log_fit)) %>%              # pull glm coefficients
  filter(term != "(Intercept)") %>%                    # drop intercept
  mutate(odds_ratio = exp(estimate))                   # optional: convert to OR

ggplot(log_coefs, aes(x = reorder(term, estimate), y = estimate)) +
  geom_col() +
  coord_flip() +
  labs(
    title = "Logistic Regression Coefficients",
    x = NULL, y = "Coefficient"
  )

#–– 3) Random Forest: Model‐based importance ––
plan("callr", workers = 6)
# 3.a) Extract the raw ranger engine
final_ranger <- final_rf_wf %>%
  last_fit(final_split) %>%
  extract_fit_engine()

# # 3.b) Make sure features are compatible with iml
mold <- extract_mold(rf_fit)

# baked_df <- juice(prep(rf_fit$pre$actions$recipe$recipe))
feat_classes <- map_chr(mold$predictors , ~ class(.x)[1])
unique(feat_classes)

# 3.c) Tell iml how to get probabilities from ranger
pfun.ranger <- function(object, newdata) {
  predict(object, data = newdata)$predictions
}

# 3.4) Build the iml Predictor
predictor.ranger <- Predictor$new(
  model       = final_ranger,
  data        = mold$predictors,
  y           = mold$outcomes$game_won,
  predict.fun = pfun.ranger)

# 3.5) Global Feature Importance (cross‐entropy loss)
imp.rf <- FeatureImp$new(predictor.ranger, 
                         loss = "ce",
                         n.repetitions = 10)
saveRDS(imp.rf, file = paste0(rds_files_path, "/Data/team_feat_import_rf.rds"))
imp.rf <- readRDS(paste0(rds_files_path, "/Data/team_feat_import_rf.rds"))

# How many top features to show
top_n <- 15
# Pull out the raw results
imp_df <- imp.rf$results %>% 
  arrange(desc(importance)) %>% # or whatever your importance column is
  slice_head(n = top_n) # Select the top N by importance

ggplot(imp_df,
       aes(x = importance,
           y = reorder(feature, importance))) +
  
  # 90 % interval (green bar in the default plot)
  geom_segment(aes(x    = importance.05,
                   xend = importance.95,
                   yend = feature),
               size = 2, colour = "darkgreen", alpha = .7) +
  
  # median importance (black dot)
  geom_point(size = 3) +
  
  geom_vline(xintercept = 1, linetype = "dashed", colour = "grey50") +
  labs(x = "Feature importance (cross-entropy ratio)",
       y = NULL) +
  theme_minimal(base_size = 12) + ggtitle("RF Feature Importance (CE loss)")


# 3.6) ALE for *all* predictors at once
predictor_names <- colnames(mold$predictors)
good_feats <- setdiff(colnames(mold$predictors), "season_new")

# 3.6a) sequential – only the small feature name is sent
ale_cols <- imp_df$feature              

plan("callr", workers = 4)
ale.rf <- FeatureEffects$new(
  predictor.ranger,
  features = imp_df$feature,
  method   = "ale"
)

plan(sequential)  # reset
gc()
saveRDS(ale.rf, file = paste0(rds_files_path, "/Data/team_ale_rf.rds"))
ale.rf <- readRDS(paste0(rds_files_path, "/Data/team_ale_rf.rds"))

# 3.6a) Grab all ALE feature names
feat_list <- names(ale.rf$results)

# 3.6b) Define a helper to plot one feature's ALE for class == 1
plot_one_ale_both <- function(feat_name) {
  df_one <- ale.rf$results[[feat_name]]
  # Plot class 1
  df_pos <- subset(df_one, .class == 1)
  df_neg <- subset(df_one, .class == 0)
  ylimits <- c(min(c(df_pos$.value,df_neg$.value)),max(c(df_pos$.value,df_neg$.value)))
  plot(df_pos$.borders, df_pos$.value,
       type = "l", lwd = 2, col = "steelblue",
       xlab = feat_name,
       ylab = "ALE (∆ P)",
       ylim = ylimits,
       main = paste0("ALE: both classes of ", feat_name))
  points(df_pos$.borders, df_pos$.value, pch = 19, col = "steelblue", cex = 0.6)
  # Add class 0 as a red line
  lines(df_neg$.borders, df_neg$.value, lwd = 2, col = "firebrick")
  points(df_neg$.borders, df_neg$.value, pch = 19, col = "firebrick", cex = 0.6)
  abline(h = 0, lty = "dashed", col = "grey50")
  legend("topleft",
         legend = c("P(y=1)", "P(y=0)"),
         col    = c("steelblue", "firebrick"),
         lwd    = 2,
         bty    = "n")
}

# 3.6c) Split into groups of 4
n     <- length(feat_list)
blocks <- split(feat_list, ceiling(seq_along(feat_list)/4))

# 3.6d) Loop over blocks of 4 features
for (block in blocks) {
  # a) Arrange a 2x2 grid
  par(mfrow = c(2,2), mar = c(4,4,2,1))  
  # mar = bottom, left, top, right margins; adjust if labels get cut off
  
  # b) Plot each of the (up to) 4 features in this block
  for (feat in block) {
    plot_one_ale_both(feat)
  }
  
  # c) Pause so you can view these four before advancing
  if (length(blocks) > 1) {
    cat("\nPress <Enter> to see the next set of 4 ALE plots...")
    readline()
  }
}
par(mfrow = c(1,1)) #reset par back to single‐figure mode

# -–– 4) Neural Network: Permutation / model‐based importance -––
# if using keras via parsnip, vip will do a model‐based VI
# 4.a) Extract the fitted Keras engine from your final workflow
nn_engine <- nn_fit %>%
  extract_fit_parsnip() %>%
  extract_fit_engine()

# 4.b) Grab the exact training‐set predictors & outcome from the workflow mold
mold     <- pull_workflow_mold(final_nn_fit)
feat_df  <- mold$predictors
y_vec    <- mold$outcomes$game_won

# 4.c) Drop unsupported Date columns (if any)
feat_df2 <- feat_df %>%
  select(-where(~ inherits(., "Date")))

# 4.d) Define a prediction wrapper for the Keras model
pfun_nn <- function(object, newdata, ...) {
  predict(object, as.matrix(newdata))
}

# 4.e) Build the iml Predictor
predictor.nn <- Predictor$new(
  model       = nn_engine,
  data        = feat_df2,
  y           = y_vec,
  predict.fun = pfun_nn
)

# 4.f) Compute global feature importance using cross-entropy loss
imp.nn <- FeatureImp$new(predictor.nn, loss = "ce")

# 4.g) Plot the importance
plot(imp.nn) +
  ggtitle("MLP Feature Importance (Cross-Entropy Loss)")

# 5) XGBoost: Model‐based importance ----
plan("callr", workers = 6)
# 5.1) Extract the raw ranger engine
final_xgboost <- final_xgb_wf %>% last_fit(final_split) %>% extract_fit_engine()

# 5.2) Make sure features are compatible with iml
mold <- extract_mold(xgb_fit)
# baked_df <- juice(prep(rf_fit$pre$actions$recipe$recipe))
feat_classes <- map_chr(mold$predictors , ~ class(.x)[1])
unique(feat_classes)

# 5.3) Tell iml how to get probabilities from ranger
pfun.xgb <- function(object, newdata) {
  # newdata must be a matrix of predictors
  modmat <- xgb.DMatrix(data = as.matrix(newdata))
  predict(object, modmat)   
}

# 5.4) Build the iml Predictor
y_num <- as.integer(as.character(mold$outcomes$game_won))
predictor.xgb <- Predictor$new(model = final_xgboost, data = mold$predictors, 
                               y = y_num, predict.fun = pfun.xgb)

# 5.5) Global Feature Importance (cross‐entropy loss) ----
imp.xgb <- FeatureImp$new(predictor.xgb, loss = "logLoss")
plan(sequential)

saveRDS(imp.xgb, file = paste0(rds_files_path, "/Data/team_feat_import_xgb.rds"))
imp.xgb <- readRDS(paste0(rds_files_path, "/Data/team_feat_import_xgb.rds"))

# How many top features to show
top_n <- length(imp.xgb$results$feature)
# Head results
imp_df <- imp.xgb$results %>%  arrange(desc(importance)) %>%
  slice_head(n = top_n) # Select the top N by importance
  # mutate(cum = cumsum(importance) / sum(importance)) %>%
  # filter(cum <= .80) 
ggplot(imp_df,aes(x = importance, y = reorder(feature, importance))) +
  # 90 % interval (green bar in the default plot)
  geom_segment(aes(x = importance.05, xend = importance.95, yend = feature),
               size = 2, colour = "darkgreen", alpha = .7) +
  geom_point(size = 3) + # median importance (black dot)
  geom_vline(xintercept = 1, linetype = "dashed", colour = "grey50") +
  labs(x = "Feature importance (log-loss ratio)", y = NULL) +
  theme_minimal(base_size = 12) + ggtitle("XGBoost Feature Importance (log loss)")

# Tail results
impt_df <- imp.xgb$results %>%  arrange(desc(importance)) %>%
  #slice_tail(n = top_n) # Select the tail N by importance
  mutate(cum = cumsum(importance) / sum(importance)) %>%
  filter(cum >= .80) 
ggplot(impt_df,aes(x = importance, y = reorder(feature, importance))) +
  # 90 % interval (green bar in the default plot)
  geom_segment(aes(x = importance.05, xend = importance.95, yend = feature),
               size = 2, colour = "darkgreen", alpha = .7) +
  geom_point(size = 3) + # median importance (black dot)
  geom_vline(xintercept = 1, linetype = "dashed", colour = "grey50") +
  labs(x = "Feature importance (log-loss ratio)", y = NULL) +
  theme_minimal(base_size = 12) + ggtitle("XGBoost Feature Importance (log loss)")

#Save Tail values for easy piping to recipe for FEATURE REMOVAL
saveRDS(impt_df, file = paste0(rds_files_path, "/Data/team_fimp_tail.rds"))

#5.6 SHAPley values
library(iml)
# 1) Compute Shapley values for a sample of rows
set.seed(42)
X_sample <- mold$predictors %>%
  sample_n(100)  # speed up
plan("callr", workers = 4)
shap <- Shapley$new(predictor.xgb, x.interest = X_sample)
plan(sequential)
saveRDS(shap, file = paste0(rds_files_path, "/Data/team_shap.rds"))

# 2) Aggregate to get mean |SHAP| per feature
shap_long <- shap$results %>%
  dplyr::group_by(feature) %>%
  dplyr::summarize(mean_abs = mean(abs(phi))) %>%
  dplyr::arrange(desc(mean_abs))

# 3) Plot global importance
library(ggplot2)
ggplot(shap_long[1:185, ], aes(x = mean_abs, y = reorder(feature, mean_abs))) +
  geom_col() +
  labs(x = "Mean |SHAP|", y = NULL, title = "Global SHAP Feature Importance (all)")

#Minimum K-Needle Point 
library(inflection) # CRAN
vec <- shap_long$mean_abs # numeric values
names(vec) <- shap_long$feature # attach feature names
vec <- sort(vec, decreasing = TRUE) # high → low

knee_idx <- function(x) {
  # x must be *cumulative* importance, 0 → 1
  n    <- length(x)
  line <- seq(0, 1, length.out = n) # diagonal reference
  which.max(abs(x - line)) # largest vertical gap
}
cum_imp <- cumsum(vec) / sum(vec) # cumulative %
knee    <- knee_idx(cum_imp) # elbow index
feat_knee    <- names(vec)[1:knee]
drop_feats  <- names(vec)[-(1:knee)]
saveRDS(drop_feats, file = paste0(rds_files_path, "/Data/team_shap_knee_drop.rds"))

#Pareto 80/20 - 90/10
vec <- shap_long$mean_abs            # ← replace with your vector
names(vec) <- shap_long$feature
vec <- sort(vec, decreasing = TRUE)  # high → low
cum_imp <- cumsum(vec) / sum(vec)          # cumulative share (0–1)
cut_off <- which(cum_imp >= 0.90)[1]        # first index beyond X %
if (is.na(cut_off)) cut_off <- length(vec) # in case total < 
sel_feats <- names(vec)[1:(cut_off - 1)]   # keep <= X %
saveRDS(sel_feats, file = paste0(rds_files_path, "/Data/team_shap_pareto.rds"))
p_drop_feats <- names(vec)[-(1:(cut_off - 1))]   # keep <= X %
saveRDS(p_drop_feats, file = paste0(rds_files_path, "/Data/team_shap_pareto_drop.rds"))

# 5.7) ALE for *all* predictors at once ----
predictor_names <- colnames(mold$predictors)
good_feats <- setdiff(colnames(mold$predictors), "season_new")

# 5.7a) sequential – only the small feature name is sent
ale_cols <- head(imp_df$feature,100)              
plan("callr", workers = 3)
# plan(sequential)  # reset
ale.xgb <- FeatureEffects$new(predictor.xgb, features = ale_cols,
                              method = "ale")
plan(sequential)
gc()
saveRDS(ale.xgb, file = paste0(rds_files_path, "/Data/team_ale_xgb.rds"))
ale.xgb <- readRDS(paste0(rds_files_path, "/Data/team_ale_xgb.rds"))

# 5.7b) Grab all ALE feature names
feat_list <- names(ale.xgb$results)

# 5.7c) Define a helper to plot one feature's ALE for class == 1
plot_one_ale_both <- function(feat_name) {
  df_one <- ale.xgb$results[[feat_name]]
  # Plot class 1
  df_pos <- subset(df_one, .class == 1)
  df_neg <- subset(df_one, .class == 0)
  ylimits <- c(min(c(df_pos$.value,df_neg$.value)),max(c(df_pos$.value,df_neg$.value)))
  plot(df_pos$.borders, df_pos$.value,
       type = "l", lwd = 2, col = "steelblue",
       xlab = feat_name,
       ylab = "ALE (∆ P)",
       ylim = ylimits,
       main = paste0("ALE: both classes of ", feat_name))
  points(df_pos$.borders, df_pos$.value, pch = 19, col = "steelblue", cex = 0.6)
  # Add class 0 as a red line
  lines(df_neg$.borders, df_neg$.value, lwd = 2, col = "firebrick")
  points(df_neg$.borders, df_neg$.value, pch = 19, col = "firebrick", cex = 0.6)
  abline(h = 0, lty = "dashed", col = "grey50")
  legend("topleft",
         legend = c("P(y=1)", "P(y=0)"),
         col    = c("steelblue", "firebrick"),
         lwd    = 2,
         bty    = "n")
}
plot_one_ale <- function(feat_name) {
  df <- ale.xgb$results[[feat_name]]
  plot(df$.borders, df$.value,
       type = "l", lwd = 2, col = "steelblue",
       xlab = feat_name, ylab = "ALE (ΔP)",
       main = paste("ALE of", feat_name))
  points(df$.borders, df$.value, pch = 19, col = "steelblue", cex = .6)
  abline(h = 0, lty = 2, col = "grey50")
}
# 5.7d) Split into groups of 4
n <- length(feat_list)
blocks <- split(feat_list, ceiling(seq_along(feat_list)/8))

# 5.7e) Loop over blocks of 4 features
for (block in blocks) {
  # a) Arrange a 2x2 grid
  par(mfrow = c(4,2), mar = c(4,4,2,1))  
  # mar = bottom, left, top, right margins; adjust if labels get cut off
  
  # b) Plot each of the (up to) 4 features in this block
  for (feat in block) {
    plot_one_ale(feat)
  }
  
  # c) Pause so you can view these four before advancing
  if (length(blocks) > 1) {
    cat("\nPress <Enter> to see the next set of 4 ALE plots...")
    readline()
  }
}
par(mfrow = c(1,1)) #reset par back to single‐figure mode


# FEATURE REDUCTION (FOR META MODEL) ---- 
library(iml)
library(ggplot2)
library(inflection) # CRAN
library(future)
library(future.callr)
library(vip)
library(ranger)
library(tidymodels)
library(tidyverse)
library(dplyr)
library(tidyr)
library(treeshap)      # install.packages("treeshap")
library(fastshap)      # install.packages("fastshap")

rds_files_path <- getwd()
final_split <- readRDS(paste0(rds_files_path, "/Data/team_final_split_v3.rds"))
final_rf_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_rf_meta.rds"))
final_xgb_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_xgb_meta.rds"))
final_nn_wf <- readRDS(paste0(rds_files_path,"/Data/team_final_wf_mlp_meta.rds"))

## Random Forest ----
plan("callr", workers = 6)
rf_last <- final_rf_wf %>% last_fit(final_split)
wf_fit  <- rf_last$.workflow[[1]]
rf_engine <- extract_fit_engine(wf_fit) # ranger object
mold <- extract_mold(wf_fit)  
feat_classes <- map_chr(mold$predictors , ~ class(.x)[1])
unique(feat_classes)
plan(sequential)

pfun.ranger <- function(object, newdata) {
  as.numeric(predict(object, data = newdata)$predictions[, 1])
}
predictor.rf <- Predictor$new(model = rf_engine,
                                  data = mold$predictors,
                                  y = as.integer(mold$outcomes$game_won), #remove integer for shapley
                                  predict.fun = pfun.ranger)

plan("callr", workers = 6)
imp.rf <- FeatureImp$new(predictor.rf, loss = "logLoss")
plan(sequential)
saveRDS(imp.rf, file = paste0(rds_files_path, "/Data/team_feat_import_rf_meta.rds"))

set.seed(123)
X_sample <- mold$predictors %>%
  sample_n(50)  # speed up
plan("multisession", workers = 6)
shap.rf <- Shapley$new(predictor.rf, x.interest = X_sample)
plan(sequential)
saveRDS(shap.rf, file = paste0(rds_files_path, "/Data/team_shap_rf_meta.rds"))
gc()

shap_long <- shap.rf$results %>%
  dplyr::group_by(feature) %>%
  dplyr::summarize(mean_abs = mean(abs(phi))) %>%
  dplyr::arrange(desc(mean_abs))

# Plot global importance
ggplot(shap_long[1:75, ], aes(x = mean_abs, y = reorder(feature, mean_abs))) +
  geom_col() +
  labs(x = "Mean |SHAP|", y = NULL, title = "Global SHAP Feature Importance (all)")

#Minimum K-Needle Point 
vec <- shap_long$mean_abs # numeric values
names(vec) <- shap_long$feature # attach feature names
vec <- sort(vec, decreasing = TRUE) # high → low

knee_idx <- function(x) {
  # x must be *cumulative* importance, 0 → 1
  n    <- length(x)
  line <- seq(0, 1, length.out = n) # diagonal reference
  which.max(abs(x - line)) # largest vertical gap
}
cum_imp <- cumsum(vec) / sum(vec) # cumulative %
knee    <- knee_idx(cum_imp) # elbow index
feat_knee    <- names(vec)[1:knee]
drop_feats  <- names(vec)[-(1:knee)]
saveRDS(drop_feats, file = paste0(rds_files_path, "/Data/team_shap_knee_drop_rf_meta.rds"))
rm(final_rf_wf,predictor.rf, rf_engine, mold, wf_fit,imp.rf)

## XGBoost ----
plan("callr", workers = 6)
xgb_last <- final_xgb_wf %>% last_fit(final_split)
wf_fit  <- xgb_last$.workflow[[1]]
xgb_engine <- extract_fit_engine(wf_fit)            # ranger object
mold <- extract_mold(wf_fit)  
feat_classes <- map_chr(mold$predictors , ~ class(.x)[1])
unique(feat_classes)

pfun.xgb <- function(object, newdata) {
  # newdata must be a matrix of predictors
  modmat <- xgb.DMatrix(data = as.matrix(newdata))
  predict(object, modmat)   
}

y_num <- as.integer(as.character(mold$outcomes$game_won))
predictor.xgb <- Predictor$new(model = xgb_engine, data = mold$predictors, 
                               y = y_num, predict.fun = pfun.xgb)

plan("callr", workers = 6)
imp.xgb <- FeatureImp$new(predictor.xgb, loss = "logLoss")
plan(sequential)
saveRDS(imp.xgb, file = paste0(rds_files_path, "/Data/team_feat_import_xgb_meta.rds"))

tail20_xgb_imp <- imp.xgb$results %>%  arrange(desc(importance)) %>%
  #slice_tail(n = top_n) # Select the tail N by importance
  mutate(cum = cumsum(importance) / sum(importance)) %>%
  filter(cum >= .80) 

set.seed(123)
X_sample <- mold$predictors %>%
  sample_n(50)  # speed up
plan("callr", workers = 6)
shap.xgb <- Shapley$new(predictor.xgb, x.interest = X_sample)
plan(sequential)
saveRDS(shap.xgb, file = paste0(rds_files_path, "/Data/team_shap_xgb_meta.rds"))
gc()
shap_long <- shap.xgb$results %>%
  dplyr::group_by(feature) %>%
  dplyr::summarize(mean_abs = mean(abs(phi))) %>%
  dplyr::arrange(desc(mean_abs))

# Plot global importance
ggplot(shap_long[1:185, ], aes(x = mean_abs, y = reorder(feature, mean_abs))) +
  geom_col() +
  labs(x = "Mean |SHAP|", y = NULL, title = "Global SHAP Feature Importance (all)")

#Minimum K-Needle Point 
vec <- shap_long$mean_abs # numeric values
names(vec) <- shap_long$feature # attach feature names
vec <- sort(vec, decreasing = TRUE) # high → low

knee_idx <- function(x) { # x must be *cumulative* importance, 0 → 1
  n    <- length(x)
  line <- seq(0, 1, length.out = n) # diagonal reference
  which.max(abs(x - line)) # largest vertical gap
}
cum_imp <- cumsum(vec) / sum(vec) # cumulative %
knee    <- knee_idx(cum_imp) # elbow index
feat_knee    <- names(vec)[1:knee]
drop_feats  <- names(vec)[-(1:knee)]
saveRDS(drop_feats, file = paste0(rds_files_path, "/Data/team_shap_knee_drop_xgb_meta.rds"))

#Pareto 80/20 - 90/10
vec <- shap_long$mean_abs  # ← replace with your vector
names(vec) <- shap_long$feature
vec <- sort(vec, decreasing = TRUE)  # high → low
cum_imp <- cumsum(vec) / sum(vec) # cumulative share (0–1)
cut_off <- which(cum_imp >= 0.90)[1] # first index beyond X %
if (is.na(cut_off)) cut_off <- length(vec) # in case total < 
sel_feats <- names(vec)[1:(cut_off - 1)]   # keep <= X %
saveRDS(sel_feats, file = paste0(rds_files_path, "/Data/team_shap_pareto_xgb_meta.rds"))
p_drop_feats <- names(vec)[-(1:(cut_off - 1))]   # keep <= X %
saveRDS(p_drop_feats, file = paste0(rds_files_path, "/Data/team_shap_pareto_drop_xgb_meta.rds"))
rm(final_xgb_wf,predictor.xgb, xgb_engine, mold, wf_fit, imp.xgb)
gc()

## Neural Net ---- 
plan("callr", workers = 6)
nn_last <- final_nn_wf %>% last_fit(final_split)
wf_fit  <- nn_last$.workflow[[1]]
nn_engine <- extract_fit_engine(wf_fit)            # ranger object
mold <- extract_mold(wf_fit)  
feat_classes <- map_chr(mold$predictors , ~ class(.x)[1])
unique(feat_classes)

pfun.nn <- function(object, newdata) {
  preds <- predict(object, as.matrix(newdata))  # first positional arg = x
  as.numeric(preds[, 1])                             # vector of class-1 probs
}
y_num <- as.integer(as.character(mold$outcomes$game_won))
predictor.nn <- Predictor$new(
  model = nn_engine,
  data = mold$predictors,
  y = y_num,
  predict.fun = pfun.nn
)

plan(sequential)
imp.nn <- FeatureImp$new(predictor.nn, loss = "logLoss")
saveRDS(imp.nn, file = paste0(rds_files_path, "/Data/team_feat_import_nn_meta.rds"))

top80_nn_imp <- imp.nn$results %>%  arrange(desc(importance)) %>%
  #slice_tail(n = top_n) # Select the tail N by importance
  mutate(cum = cumsum(importance) / sum(importance)) %>%
  filter(cum <= .80) 
drop_feats <- readRDS(paste0(rds_files_path, "/Data/team_shap_knee_drop_nn_meta.rds"))

core_features <- intersect(drop_feats,top80_nn_imp$feature)

set.seed(123)
X_sample <- mold$predictors %>%
  sample_n(50)  # speed up
plan("callr", workers = 6)
shap.nn <- Shapley$new(predictor.nn, x.interest = X_sample)
plan(sequential)
saveRDS(shap.nn, file = paste0(rds_files_path, "/Data/team_shap_nn_meta.rds"))
shap_long <- shap.nn$results %>%
  dplyr::group_by(feature) %>%
  dplyr::summarize(mean_abs = mean(abs(phi))) %>%
  dplyr::arrange(desc(mean_abs))

# Plot global importance
ggplot(shap_long[1:185, ], aes(x = mean_abs, y = reorder(feature, mean_abs))) +
  geom_col() +
  labs(x = "Mean |SHAP|", y = NULL, title = "Global SHAP Feature Importance (all)")

#Minimum K-Needle Point 
vec <- shap_long$mean_abs # numeric values
names(vec) <- shap_long$feature # attach feature names
vec <- sort(vec, decreasing = TRUE) # high → low

knee_idx <- function(x) {
  # x must be *cumulative* importance, 0 → 1
  n    <- length(x)
  line <- seq(0, 1, length.out = n) # diagonal reference
  which.max(abs(x - line)) # largest vertical gap
}
cum_imp <- cumsum(vec) / sum(vec) # cumulative %
knee    <- knee_idx(cum_imp) # elbow index
feat_knee    <- names(vec)[1:knee]
drop_feats  <- names(vec)[-(1:knee)]
saveRDS(drop_feats, file = paste0(rds_files_path, "/Data/team_shap_knee_drop_nn_meta.rds"))
rm(final_nn_wf, predictor.nn, nn_engine, mold, wf_fit)

# GLM ---- 
