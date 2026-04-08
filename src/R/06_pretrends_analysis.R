rm(list=ls())

library(yaml)
library(broom)
library(dplyr)
library(arrow)
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

fee_chg_pos_F <- paste0("fee_chg_pos_F",12:1)
fee_chg_pos_L <- paste0("fee_chg_pos_L",1:12)
fee_chg_neg_F <- paste0("fee_chg_neg_F",12:1)
fee_chg_neg_L <- paste0("fee_chg_neg_L",1:12)

# ---- Quality pretrends check

qual_df <- read_parquet(paste0(DATA_PATH, "/pretrends_check_qual_df.parquet"))

qual_df$log_sats48 <- log(1+qual_df$sats48)

fmla1 <- build_fmla(
  "log_sats48",
  c(fee_chg_pos_F, "fee_chg_pos", fee_chg_pos_L, fee_chg_neg_F, "fee_chg_neg", fee_chg_neg_L),
  c("weekId", "sub_subOwner_id")
)
fmla2 <- build_fmla(
  "log_sats48",
  c(fee_chg_pos_F, "fee_chg_pos", fee_chg_pos_L, fee_chg_neg_F, "fee_chg_neg", fee_chg_neg_L),
  c("weekId", "sub_subOwner_id", "userId")
)

r1 <- feols(fmla1, data=qual_df, vcov=~subId)
r2 <- feols(fmla2, data=qual_df, vcov=~subId)


# ---- Quantity pretrends check

quant_df <- read_parquet(paste0(DATA_PATH, "/pretrends_check_quant_df.parquet"))

quant_df$log_posts <- log(1+quant_df$n_posts)

fmla3 <- build_fmla(
  "log_posts",
  c(fee_chg_pos_F, "fee_chg_pos", fee_chg_pos_L, fee_chg_neg_F, "fee_chg_neg", fee_chg_neg_L),
  c("weekId", "sub_subOwner_id")
)

r3 <- feols(fmla3, data=quant_df, vcov=~subId)


# ---- Output results

etable(r1, r2, r3)

coefs_df <- rbind(
  extract_reg(r1, "r1"),
  extract_reg(r2, "r2"),
  extract_reg(r3, "r3")
)

outfile <- paste0(DATA_PATH, "/pretrends_analysis_coefs.parquet")
write_parquet(coefs_df, outfile)





