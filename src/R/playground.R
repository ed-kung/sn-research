rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

ADDED_LINES <- list(
  c("Territory FE",       "N", "Y", "Y", "Y"),
  c("Quarter FE",         "N", "Y", "Y", "N"),
  c("Week FE",            "N", "N", "N", "Y"),
  c("User FE",            "N", "N", "Y", "Y")
)

filename <- paste0(DATA_PATH, "/zap_analysis_data.csv")

df <- read.csv(filename)

df$log_sats <- log(df$sats)
df$log_price <- log(df$price_mid)

r0 <- felm(log_sats ~ log_price, data=df)
r1 <- felm(log_sats ~ log_price | subId + qtrId, data=df)
r2 <- felm(log_sats ~ log_price | subId + qtrId + userId, data=df)
r3 <- felm(log_sats ~ log_price | subId + weekId + userId, data=df)

stargazer(
  r0, r1, r2, r3, type="text",
  add.lines = ADDED_LINES
)

