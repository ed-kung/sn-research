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
  c("Week FE",      "N", "N", "Y", "Y")
)

filename <- paste0(DATA_PATH, "/profitability-analysis.parquet")

df <- read_parquet(filename)

df$log_items <- log(1+df$rolling_items)
df$unprofitable <- df$rolling_profit1 < 0
df$unprofitable_X_mom2_growth <- df$unprofitable * df$mom2_growth
df$new_account <- df$account_age<=9


r1 <- felm(became_inactive ~ unprofitable, data=df)
r2 <- felm(became_inactive ~ unprofitable + log_items, data=df)
r3 <- felm(became_inactive ~ unprofitable + log_items | weekId, data=df)
r4 <- felm(became_inactive ~ unprofitable + unprofitable_X_mom2_growth + log_items | weekId, data=df)

out_tbl <- stargazer(
  r1, r2, r3, r4, type=TABLE_TYPE,
  covariate.labels = c(
    "Unprofitable in last 8 weeks", 
    "$\\ldots$ $\\times$ BTC price appreciation in last 8 weeks",
    "log(Items posted in last 8 weeks)"
  ),
  add.lines = ADDED_LINES
)

if (TABLE_TYPE=="latex") {
  outlines <- out_tbl[c(15:29, 33:34)]
  outfile <- paste0(LOCAL_PATH, "/results/tbl_profitability_analysis.tex")
  writeLines(outlines, outfile)
}

outfile <- paste0(DATA_PATH, "/profitability_analysis.csv")
write.csv(as.data.frame(coef(r4)), outfile)

