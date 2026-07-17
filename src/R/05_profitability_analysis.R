rm(list=ls())

library(yaml)
library(stargazer)
library(dplyr)
library(arrow)
library(broom)
library(fixest)
options(width=10000)

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

# --- Load and clean data

filename <- paste0(DATA_PATH, "/profitability-analysis.parquet")

df <- read_parquet(filename)

df$log_items <- log(1+df$rolling_items)
df$unprofitable <- df$rolling_profit1 < 0
df$unprofitable_X_mom2_growth <- df$unprofitable * df$mom2_growth
df$new_account <- df$account_age<=9

# --- Run regressions

r1 <- feols(became_inactive ~ unprofitable, data=df, vcov = ~userId)
r2 <- feols(became_inactive ~ unprofitable + log_items, data=df, vcov = ~userId)
r3 <- feols(became_inactive ~ unprofitable + log_items | weekId, data=df, vcov = ~userId)
r4 <- feols(became_inactive ~ unprofitable + unprofitable_X_mom2_growth + log_items | weekId, data=df, vcov = ~userId)

etable(r1, r2, r3, r4)

coef_df <- rbind(
  extract_reg(r1, "r1"),
  extract_reg(r2, "r2"),
  extract_reg(r3, "r3"),
  extract_reg(r4, "r4")
)

outfile <- paste0(DATA_PATH, "/profitability_analysis_coefs.parquet")
write_parquet(coef_df, outfile)

