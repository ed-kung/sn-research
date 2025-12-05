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

filename <- paste0(DATA_PATH, "/v4v_analysis_data.parquet")

df <- read_parquet(filename)

df$log_sats48 <- log(1+df$sats48)

r0 <- felm(future_text_dist ~ log_sats48, data=df)

stargazer(r0, type="text")


