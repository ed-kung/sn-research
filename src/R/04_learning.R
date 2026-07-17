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
df$time <- df$weekId / max(df$weekId)  # linear time trend normalized to 0-1
df$subId <- as.factor(df$subId)

# rename some variables for faster iteration
df$sig <- df$cum_avg_lnsats48
df$sigr <- df$cum_avg_lnsats48_recent
df$sigu <- df$cum_avg_lnsats48_user
df$siga <- df$cum_avg_lnsats48_activity 
df$exp <- log1p(df$experience_posts)

r0 <- mclogit(cbind(chosen,itemId) ~ sig + sig:exp, random=~1 | post_type/subId, data=df)
summary(r0)
r1 <- mclogit(cbind(chosen,itemId) ~ sig + sig:exp + sigu + sigu:exp, random=~1 | post_type/subId, data=df)
summary(r1)
r2 <- mclogit(cbind(chosen,itemId) ~ sig + sig:exp + sigr + sigr:exp, random=~1 | post_type/subId, data=df)
summary(r2)
r3 <- mclogit(cbind(chosen,itemId) ~ sig + sig:exp + siga + siga:exp, random=~1 | post_type/subId, data=df)
summary(r3)



r1 <- mclogit(cbind(chosen,itemId) ~ sigr + sigr:exp, random=~1 | post_type/subId, data=df)
summary(r1)
r1b <- mclogit(cbind(chosen,itemId) ~ sigr + sigr:exp + sigu + sigu:exp, random=~1 | post_type/subId, data=df)
summary(r1b)

r2 <- mclogit(cbind(chosen,itemId) ~ siga + siga:exp, random=~1 | post_type/subId, data=df)
summary(r2)
r2b <- mclogit(cbind(chosen,itemId) ~ siga + siga:exp + sigu + sigu:exp, random=~1 | post_type/subId, data=df)
summary(r2b)





regfunc <- function(signal_var, exp_var) {
  df$signal <- df[[signal_var]]
  df$exp <- log1p(df[[exp_var]])
  df$signal_x_exp <- df$signal * df$exp
  fit <- mclogit(
    cbind(chosen, itemId) ~ signal + signal_x_exp + post_type:time,
    random = ~ 1 | post_type/subId,
    data = df
  )
  return(fit)
}

r0 <- mclogit(cbind(chosen,itemId) ~ cum_avg_lnsats48 + cum_avg_lnsats48*log1p(experience_posts) + post_type:linear_time, random = ~ 1 | post_type/subId, data=df)
summary(r0)
r1 <- mclogit(cbind(chosen,itemId) ~ cum_avg_lnsats48 + cum_avg_lnsats48*log1p(experience_posts) + cum_avg_lnsats48_user + cum_avg_lnsats48_user*log1p(experience_posts) + post_type:linear_time, random = ~ 1 | post_type/subId, data=df)
summary(r1)


#r0 <- regfunc("cum_avg_lnsats48", "experience_posts")
#r1 <- regfunc("cum_avg_lnsats48_user", "experience_posts")
#r2 <- regfunc("cum_avg_lnsats48_recent", "experience_posts")
#r3 <- regfunc("cum_avg_lnsats48_activity", "experience_posts")

keepvars <- c("signal", "signal_x_exp")

stargazer(
  r0, r1, r2, r3, type="text",
  keep=keepvars
)



