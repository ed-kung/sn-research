rm(list=ls())

library(yaml)
library(arrow)
library(broom)
library(dplyr)
library(fixest)
library(mclogit)
library(stargazer)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
LOCAL_PATH <- LOCAL_CONFIG["LOCAL_PATH"][[1]]
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

# ---- Helper functions

# regression function
reg_func <- function(signal_var, experience_var, surprise_var, data) {
  data$signal <- data[[signal_var]]
  data$exp <- log1p(data[[experience_var]])
  data$signal_x_exp <- data$signal * data$exp
  if (nchar(surprise_var)==0) {
    fmla <- as.formula("cbind(chosen, itemId) ~ signal + signal_x_exp + post_type:time")
  } else {
    data$surpr <- data[[surprise_var]]
    fmla <- as.formula("cbind(chosen, itemId) ~ signal + signal_x_exp + surpr + post_type:time")
  }
  result <- mclogit(fmla, random = ~1 | post_type/subId, data=data)
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
    estimate = c(nobs(reg), 1 - (reg$deviance / reg$null.deviance)),
    serr = NA_real_
  )
  return(rbind(coef_df, stats_df))
}

# ---- Data loading and cleaning

in_filename <- paste0(DATA_PATH, "/learning_analysis_data.parquet")

df <- read_parquet(in_filename)

# filter out na values
df <- filter(df, !is.na(df$cum_avg_lnsats48))
df <- filter(df, !is.na(df$cum_avg_lnsats48_recent))
df <- filter(df, !is.na(df$cum_avg_lnsats48_user))
df <- filter(df, !is.na(df$cum_avg_lnsats48_activity))

df$post_type <- as.factor(df$post_type)
df$time <- df$weekId / max(df$weekId)  # linear time trend normalized to 0-1
df$subId <- as.factor(df$subId)
df$surprise <- df$cum_avg_lnsats48_user - df$cum_avg_lnsats48

r0 <- reg_func("cum_avg_lnsats48", "experience_posts", "", df)
r1 <- reg_func("cum_avg_lnsats48_recent", "experience_posts", "", df)
r2 <- reg_func("cum_avg_lnsats48_activity", "experience_posts", "", df)
r3 <- reg_func("cum_avg_lnsats48", "experience_posts", "surprise", df)
r4 <- reg_func("cum_avg_lnsats48_recent", "experience_posts", "surprise", df)
r5 <- reg_func("cum_avg_lnsats48_activity", "experience_posts", "surprise", df)

summary(r0)
summary(r1)
summary(r2)
summary(r3)
summary(r4)
summary(r5)

coefs_df <- rbind(
  extract_reg(r0, "r0"),
  extract_reg(r1, "r1"),
  extract_reg(r2, "r2"),
  extract_reg(r3, "r3"),
  extract_reg(r4, "r4"),
  extract_reg(r5, "r5")
)

out_filename <- paste0(DATA_PATH, "/learning_analysis_coefs.parquet")
write_parquet(coefs_df, out_filename)

