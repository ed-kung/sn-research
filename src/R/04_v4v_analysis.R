rm(list=ls())

library(yaml)
library(arrow)
library(broom)
library(dplyr)
library(fixest)

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

in_filename <- paste0(DATA_PATH, "/v4v-analysis-2.parquet")

df <- read_parquet(in_filename)
df$log_sats48 <- log(1+df$sats48)
df$log_numwords <- log(1 + df$num_words)

# ---- Run the regressions

MAIN_VARS <- c("log_numwords", "no_text", "is_link_post", "num_img_or_links")

TEXT_PCA_K <- 10
TEXT_PCA_VARS <- paste0("text_emb_", 0:(TEXT_PCA_K-1))

yvar <- "log_sats48"
covars <- c(MAIN_VARS, TEXT_PCA_VARS)

r1 <- feols(build_fmla(yvar, MAIN_VARS), data=df, vcov = ~subId)
r2 <- feols(build_fmla(yvar, covars), data=df, vcov = ~subId)
r3 <- feols(build_fmla(yvar, covars, c("subId", "userId")), data=df, vcov = ~subId)

etable(r1, r2, r3)  # show results


# ---- Convert coefficients to dataframe

coefs_df <- rbind(
  extract_reg(r1, "r1"),
  extract_reg(r2, "r2"),
  extract_reg(r3, "r3")
)

out_filename <- paste0(DATA_PATH, "/v4v-analysis-2-zreg.parquet")
write_parquet(coefs_df, out_filename)


# ---- Extract values for q and surprise

df$q <- df$log_sats48
df$q[r3$obs_selection$obsRemoved] <- predict(r3)
df$surprise <- df$log_sats48 - df$q

out_filename <- paste0(DATA_PATH, "/v4v-analysis-2-data-w-q.parquet")
write_parquet(df, out_filename)


# ---- Calculate running average or surprise

df <- df %>%
  arrange(userId, itemId) %>%
  group_by(userId) %>%
  mutate(
    avg_surprise = lag(cummean(surprise)),
    log_prior_zaps = lag(log(1+cumsum(sats48))),
    log_prior_posts = lag(row_number())
  ) %>%
  ungroup()


# ---- Regress q on surprise

yvar = "q"
covars = c("avg_surprise")

r4 <- feols(
  build_fmla(
    yvar, 
    c("avg_surprise"),
    c("subId", "userId")
  ), 
  data=df, vcov = ~subId
)
r5 <- feols(
  build_fmla(
    yvar, 
    c("avg_surprise", "log_prior_zaps", "log_prior_posts"), 
    c("subId", "userId")
  ), 
  data=df, vcov = ~subId
)
r6 <- feols(
  build_fmla(
    yvar, 
    c("avg_surprise", "log_prior_zaps", "log_prior_posts"), 
    c("subId", "userId", "weekId")
  ), 
  data=df, vcov = ~subId
)

etable(r4, r5, r6)

coefs_df <- rbind(
  extract_reg(r4, "r4"),
  extract_reg(r5, "r5"),
  extract_reg(r6, "r6")
)

out_filename <- paste0(DATA_PATH, "/v4v-analysis-2-qreg.parquet")
write_parquet(coefs_df, out_filename)






