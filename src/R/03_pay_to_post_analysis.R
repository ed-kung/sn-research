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


# ---- Data loading and cleaning

qual_file <- paste0(DATA_PATH, "/post_quality_analysis_data.parquet")
quant_file <- paste0(DATA_PATH, "/post_quantity_analysis_data.parquet")

qual_df = read_parquet(qual_file)
quant_df = read_parquet(quant_file)

qual_df$log_sats48 <- log(1 + qual_df$sats48)
qual_df$log_cost <- log(1 + qual_df$cost)
qual_df$log_comments48 <- log(1 + qual_df$comments48)
qual_df <- qual_df %>% group_by(
  subId, weekId
) %>% mutate(
  num_posts = n()
) %>% ungroup()
qual_df$log_num_posts <- log(1 + qual_df$num_posts)

quant_df$log_cost <- log(1 + quant_df$posting_fee)
quant_df$log_posts <- log(1 + quant_df$n_posts)



# ---- Quality Regression (Zaps)

rzaps0 <- feols(log_sats48 ~ log_cost | 0, data=qual_df, vcov = ~subId)
rzaps1 <- feols(log_sats48 ~ log_cost | subId + weekId, data=qual_df, vcov = ~subId)
rzaps2 <- feols(log_sats48 ~ log_cost + log_num_posts | subId + weekId, data=qual_df, vcov = ~subId)
rzaps3 <- feols(log_sats48 ~ log_cost + log_num_posts | subId + weekId + userId, data=qual_df, vcov = ~subId)

etable(rzaps0, rzaps1, rzaps2, rzaps3)

coefs_df <- rbind(
  extract_reg(rzaps0, "rzaps0"),
  extract_reg(rzaps1, "rzaps1"),
  extract_reg(rzaps2, "rzaps2"),
  extract_reg(rzaps3, "rzaps3")
)

outfile <- paste0(DATA_PATH, "/pay_to_post_quality_analysis_coefs.parquet")
write_parquet(coefs_df, outfile)



# ---- Quantity Regressions

rquan0 <- feols(log_posts ~ log_cost | 0, data=quant_df, vcov = ~subId)
rquan1 <- feols(log_posts ~ log_cost | subId + weekId, data=quant_df, vcov = ~subId)
rquan2 <- feols(log_posts ~ log_cost | weekId + sub_subOwner_id, data=quant_df, vcov = ~subId)

etable(rquan0, rquan1, rquan2)

coefs_df <- rbind(
  extract_reg(rquan0, "rquan0"),
  extract_reg(rquan1, "rquan1"),
  extract_reg(rquan2, "rquan2")
)

outfile <- paste0(DATA_PATH, "/pay_to_post_quantity_analysis_coefs.parquet")
write_parquet(coefs_df, outfile)


