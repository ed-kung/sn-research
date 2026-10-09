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
    coef_name = c("num_obs", "R2"),
    estimate = c(reg$nobs, r2(reg, "pr2")),
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

in_filename <- paste0(DATA_PATH, "/temp.parquet")

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


