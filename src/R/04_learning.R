rm(list=ls())

library(yaml)
library(arrow)
library(broom)
library(dplyr)
library(fixest)
library(sandwich)
library(mlogit)
library(stargazer)
#library(lfe)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
LOCAL_PATH <- LOCAL_CONFIG["LOCAL_PATH"][[1]]
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

# ---- Helper functions

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
    estimate = c(reg$nobs, r2(reg, "pr2")),
    serr = NA_real_
  )
  return(rbind(coef_df, stats_df))
}




# ---- Data loading and cleaning

in_filename <- paste0(DATA_PATH, "/learning_analysis_data.parquet")

df <- read_parquet(in_filename)

r1 <- fepois(post_count ~ K1 | user_week_id + user_sub_id + sub_week_id, data=df, vcov=~sub_week_id)
r2 <- fepois(post_count ~ K2 | user_week_id + user_sub_id + sub_week_id, data=df, vcov=~sub_week_id)
r3 <- fepois(post_count ~ K3 | user_week_id + user_sub_id + sub_week_id, data=df, vcov=~sub_week_id)
r4 <- fepois(post_count ~ K1 + K2 + K3 | user_week_id + user_sub_id + sub_week_id, data=df, vcov=~sub_week_id)
etable(r1, r2, r3, r4)

coefs_df <- rbind(
  extract_reg(r1, "r1"),
  extract_reg(r2, "r2"),
  extract_reg(r3, "r3"),
  extract_reg(r4, "r4")
)

outfile <- paste0(DATA_PATH, "/learning_regs.parquet")
write_parquet(coefs_df, outfile)


