rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)
library(arrow)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
LOCAL_PATH <- LOCAL_CONFIG["LOCAL_PATH"][[1]]
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]
TABLE_TYPE <- "latex"

ADDED_LINES <- list(
  c("Territory FE",       "N", "Y", "Y", "Y"),
  c("Week FE",            "N", "N", "Y", "Y"),
  c("Territory Owner FE", "N", "N", "N", "Y")
)

qual_file <- paste0(DATA_PATH, "/post_quality_analysis_data.parquet")
quant_file <- paste0(DATA_PATH, "/post_quantity_analysis_data.parquet")

qual_df = read_parquet(qual_file)
quant_df = read_parquet(quant_file)

qual_df$log_sats48 <- log(1 + qual_df$sats48)
qual_df$log_cost <- log(1 + qual_df$cost)
qual_df$log_comments48 <- log(1 + qual_df$comments48)

quant_df$log_cost <- log(1 + quant_df$posting_fee)
quant_df$log_posts <- log(1 + quant_df$n_posts)

# ---- Quality Regression (Zaps)

rzaps0 <- felm(log_sats48 ~ log_cost, data=qual_df)
rzaps1 <- felm(log_sats48 ~ log_cost | subId, data=qual_df)
rzaps2 <- felm(log_sats48 ~ log_cost | subId + weekId, data=qual_df)
rzaps3 <- felm(log_sats48 ~ log_cost | weekId + sub_subOwner_id, data=qual_df)

zaps_tbl <- stargazer(
  rzaps0, rzaps1, rzaps2, rzaps3, type=TABLE_TYPE,
  covariate.labels = "log(Posting Cost)",
  add.lines = ADDED_LINES
)

if (TABLE_TYPE=="latex") {
  outlines <- zaps_tbl[c(15:25, 29:30)]
  outfile <- paste0(LOCAL_PATH, "/results/tbl_sats48_cost_reg.tex")
  writeLines(outlines, outfile)
}

outfile <- paste0(DATA_PATH, "/sats48_cost_reg.csv")
write.csv(as.data.frame(coef(rzaps3)), outfile)

# ---- Quality Regression (Comments)

rcomm0 <- felm(log_comments48 ~ log_cost, data=qual_df)
rcomm1 <- felm(log_comments48 ~ log_cost | subId, data=qual_df)
rcomm2 <- felm(log_comments48 ~ log_cost | subId + weekId, data=qual_df)
rcomm3 <- felm(log_comments48 ~ log_cost | weekId + sub_subOwner_id, data=qual_df)

comm_tbl <- stargazer(
  rcomm0, rcomm1, rcomm2, rcomm3, type=TABLE_TYPE,
  covariate.labels = "log(Posting Cost)",
  add.lines = ADDED_LINES
)

if (TABLE_TYPE=="latex") {
  outlines <- comm_tbl[c(15:25, 29:30)]
  outfile <- paste0(LOCAL_PATH, "/results/tbl_comments48_cost_reg.tex")
  writeLines(outlines, outfile)
}

outfile <- paste0(DATA_PATH, "/comments48_cost_reg.csv")
write.csv(as.data.frame(coef(rcomm3)), outfile)

# ---- Quantity Regressions

rquan0 <- felm(log_posts ~ log_cost, data=quant_df)
rquan1 <- felm(log_posts ~ log_cost | subId, data=quant_df)
rquan2 <- felm(log_posts ~ log_cost | subId + weekId, data=quant_df)
rquan3 <- felm(log_posts ~ log_cost | weekId + sub_subOwner_id, data=quant_df)

quan_tbl <- stargazer(
  rquan0, rquan1, rquan2, rquan3, type=TABLE_TYPE,
  covariate.labels = "log(Posting Cost)",
  add.lines = ADDED_LINES
)

if (TABLE_TYPE=="latex") {
  outlines <- quan_tbl[c(15:25, 29:30)]
  outfile <- paste0(LOCAL_PATH, "/results/tbl_posts_cost_reg.tex")
  writeLines(outlines, outfile)
}

outfile <- paste0(DATA_PATH, "/posts_cost_reg.csv")
write.csv(as.data.frame(coef(rquan3)), outfile)


