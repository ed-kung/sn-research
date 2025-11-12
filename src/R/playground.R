rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)
library(arrow)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

filename1 <- paste0(DATA_PATH, "/tempdf1.parquet")
filename2 <- paste0(DATA_PATH, "/tempdf2.parquet")

df1 <- read_parquet(filename1)
df2 <- read_parquet(filename2)

add_vars <- function(df) {
  df$profitable <- df$rolling_profit0 > 0
  df$logprofit <- df$log_rolling_profit0
  df$logposts <- df$log_rolling_posts

  df$profitable_X_noncustodial <- df$profitable * df$sn_is_noncustodial
  df$logprofit_X_noncustodial <- df$logprofit * df$sn_is_noncustodial
  df$logposts_X_noncustodial <- df$logposts * df$sn_is_noncustodial
  
  return(df)
}

df1 <- add_vars(df1)
df2 <- add_vars(df2)

run_regs <- function(df, yvar) {
  fmla1 <- as.formula(paste0(yvar, " ~ sn_is_noncustodial"))
  fmla2 <- as.formula(paste0(yvar, " ~ sn_is_noncustodial + profitable + profitable_X_noncustodial"))
  fmla3 <- as.formula(paste0(yvar, " ~ profitable + profitable_X_noncustodial | week"))
  fmla4 <- as.formula(paste0(yvar, " ~ profitable + profitable_X_noncustodial + logposts + logposts_X_noncustodial | week"))
  
  r1 <- felm(fmla1, data=df)
  r2 <- felm(fmla2, data=df)
  r3 <- felm(fmla3, data=df)
  r4 <- felm(fmla4, data=df)
  
  stargazer(
    r1, r2, r3, r4, type="text",
    add.lines=list(
      c("Week FE", "N", "N", "Y", "Y")
    )
  )
}

run_regs(df1, "attached_recv_wallet")



r1 <- felm(
  attached_recv_wallet ~ sn_is_noncustodial, data = df1
)
r2 <- felm(
  attached_recv_wallet ~ sn_is_noncustodial + profitable, data = df1
)
r3 <- felm(
  attached_recv_wallet ~ sn_is_noncustodial + profitable + profitable_X_noncustodial, data = df1
)
r4 <- felm(
  attached_recv_wallet ~ sn_is_noncustodial + profitable + profitable_X_noncustodial | week, data = df1
)
r5 <- felm(
  attached_recv_wallet ~ profitable + profitable_X_noncustodial + log_posts + log_posts_X_noncustodial | week, data = df1
)

stargazer(r1, r2, r3, r4, r5, type="text")
