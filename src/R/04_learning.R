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

# building formulas
build_fmla <- function(yvar, covars, fevars=c()) {
  if (length(covars)>0) {
    covars_fmla <- paste(covars, collapse = " + ")
  }
  else {
    covars_fmla <- "1"
  }
  if (length(fevars)>0) {
    fevars_fmla <- paste(fevars, collapse = " + ")
  }
  else {
    fevars_fmla <- "0"
  }
  as.formula(paste(yvar, " ~ ", covars_fmla, " | ", fevars_fmla))
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
    coef_name = c("num_obs", "R2"),
    estimate = c(reg$nobs, fitstat(reg, "r2")[[1]]),
    serr = NA_real_
  )
  return(rbind(coef_df, stats_df))
}



# ---- Data loading and cleaning

in_filename <- paste0(DATA_PATH, "/learning_analysis_data.parquet")

df <- read_parquet(in_filename)

df$post_type <- as.factor(df$post_type)
df$linear_time <- df$weekId / max(df$weekId)

regfunc <- function(signal_var, exp_var) {
  df$signal <- df[[signal_var]]
  df$exp <- log1p(df[[exp_var]])
  df$signal_x_exp <- df$signal * df$exp
  fit <- mclogit(
    cbind(chosen, itemId) ~ signal + signal_x_exp + post_type*linear_time,
    data = df
  )
  return(fit)
}

r0 <- regfunc("cum_avg_lnsats48", "experience_posts")
r1 <- regfunc("cum_avg_lnsats48_user", "experience_posts")
r2 <- regfunc("cum_avg_lnsats48_recent", "experience_posts")
r3 <- regfunc("cum_avg_lnsats48_activity", "experience_posts")

keepvars <- c("signal", "signal_x_exp")

stargazer(
  r0, r1, r2, r3, type="text",
  keep=keepvars
)



