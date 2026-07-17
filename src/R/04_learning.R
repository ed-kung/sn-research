rm(list=ls())

library(yaml)
library(arrow)
library(broom)
library(dplyr)
library(fixest)
library(sandwich)
library(mlogit)
library(stargazer)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
LOCAL_PATH <- LOCAL_CONFIG["LOCAL_PATH"][[1]]
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

# ---- Helper functions

# regression function
reg_func <- function(signal_var, experience_var, user_signal_var, data) {
  data$signal <- data[[signal_var]]
  data$exp <- log1p(data[[experience_var]])
  data$signal_x_exp <- data$signal * data$exp
  if (nchar(user_signal_var)==0) {
    fmla <- as.formula("chosen ~ signal + signal_x_exp | time")
  } else {
    data$surpr <- data[[user_signal_var]] - data[[signal_var]]
    fmla <- as.formula("chosen ~ signal + signal_x_exp + surpr | time")
  }
  result <- mlogit(fmla, data=data)
}

# extracting regression results
extract_reg <- function(reg, reg_name) {
  # coefficients
  tidy_df <- tidy(reg)
  coef_df <- data.frame(
    regression_name = reg_name, 
    coef_name = tidy_df$term,
    estimate = tidy_df$estimate,
    serr = tidy_df$std.error
  )
  # stats
  stats_df <- data.frame(
    regression_name = reg_name,
    coef_name = c("num_obs", "pseudo_r2"),
    estimate = c(NROW(estfun(reg)), summary(reg)$mfR2),
    serr = NA_real_
  )
  return(rbind(coef_df, stats_df))
}


# ---- Functions for clustering standard errors

# get choice-occassion ids in model-frame order
chids_of <- function(reg) {
  ix <- idx(reg$model)
  unique(as.character(ix[[1]]))
}

# build cluster vector for fitted model from an itemId -> userId lookup
cluster_of <- function(reg, id_map) {
  cl <- unname(id_map[chids_of(reg)])
  stopifnot(!anyNA(cl), length(cl) == NROW(estfun(reg)))
  cl
}

# return a copy of model whose vcov() is clustered
cluster_se <- function(reg, id_map) {
  V <- vcovCL(reg, cluster = cluster_of(reg, id_map), type = "HC0", cadjust = TRUE)
  out <- reg
  out$hessian <- -solve(V)
  out
}




# ---- Data loading and cleaning

in_filename <- paste0(DATA_PATH, "/learning_analysis_data.parquet")

df <- read_parquet(in_filename)
df$post_type <- as.factor(df$post_type)
df$time <- df$weekId / max(df$weekId)  # linear time trend normalized to 0-1

# build itemId -> userId lookup
item_user <- df %>%
  distinct(itemId, userId) %>%
  mutate(itemId = as.character(itemId), userId = as.character(userId))
stopifnot(!any(duplicated(item_user$itemId)))
id_map <- setNames(item_user$userId, item_user$itemId)

dfm <- dfidx(
  df, 
  idx = c("itemId", "post_type"),  # first index is a choice occassion, second index is alternatives
  choice = "chosen",
  shape = "long"
)

r0 <- reg_func("cum_avg_lnsats48", "experience_posts", "", dfm)
r1 <- reg_func("cum_avg_lnsats48_recent", "experience_posts", "", dfm)
r2 <- reg_func("cum_avg_lnsats48_activity", "experience_posts", "", dfm)
r3 <- reg_func("cum_avg_lnsats48", "experience_posts", "cum_avg_lnsats48_u", dfm)
r4 <- reg_func("cum_avg_lnsats48_recent", "experience_posts", "cum_avg_lnsats48_recent_u", dfm)
r5 <- reg_func("cum_avg_lnsats48_activity", "experience_posts", "cum_avg_lnsats48_activity_u", dfm)

r0c <- cluster_se(r0, id_map)
r1c <- cluster_se(r1, id_map)
r2c <- cluster_se(r2, id_map)
r3c <- cluster_se(r3, id_map)
r4c <- cluster_se(r4, id_map)
r5c <- cluster_se(r5, id_map)

stargazer(
  r0c, r1c, r2c, r3c, r4c, r5c, type="text",
  keep=c("signal", "signal_x_exp", "surpr")
)

coefs_df <- rbind(
  extract_reg(r0c, "r0"),
  extract_reg(r1c, "r1"),
  extract_reg(r2c, "r2"),
  extract_reg(r3c, "r3"),
  extract_reg(r4c, "r4"),
  extract_reg(r5c, "r5")
)

out_filename <- paste0(DATA_PATH, "/learning_analysis_coefs.parquet")
write_parquet(coefs_df, out_filename)

