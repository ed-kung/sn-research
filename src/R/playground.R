rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)
library(arrow)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

filename <- paste0(DATA_PATH, "/objective_quality_analysis.parquet")

df <- read_parquet(filename)

df$log_sats48 <- log(1+df$sats48)
df$log_words <- log(1+df$num_words)
df$log_img_links <- log(1+df$num_img_or_links)
df$log_in_degree <- log(1+df$in_degree)
df$log_out_degree <- log(1+df$out_degree)

r0 <- lm(log_sats48 ~ log_words + log_img_links + is_link_post + log_in_degree, data=df)

stargazer(r0, type="text")



