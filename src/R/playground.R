rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

filename <- paste0(DATA_PATH, "/posts_quality_regs.csv")

df <- read.csv(filename)

df$log_sats48 <- log(1+df$sats48)
df$log_ncomments48 <- log(1+df$n_comments48)
df$log_cost <- log(df$cost)

reg_sats_0 <- felm(log_sats48 ~ log_cost, data=df)
reg_sats_1 <- felm(log_sats48 ~ log_cost | subId, data=df)
reg_sats_2 <- felm(log_sats48 ~ log_cost | subId + weekId, data=df)
reg_sats_3 <- felm(log_sats48 ~ log_cost | subId + weekId + userId, data=df)

stargazer(
  reg_sats_0, reg_sats_1, reg_sats_2, reg_sats_3, 
  type="text",
  add.lines=list(
    c("Territory FE", "N", "Y", "Y", "Y"),
    c("Week FE",      "N", "N", "Y", "Y"),
    c("User FE",      "N", "N", "N", "Y")
  )
)
fe_df <- getfe(reg_sats_2)
fe_filename <- paste0(DATA_PATH, "/reg_sats_2.csv")
write.csv(fe_df, fe_filename, row.names=FALSE)

reg_ncomments_0 <- felm(log_ncomments48 ~ log_cost, data=df)
reg_ncomments_1 <- felm(log_ncomments48 ~ log_cost | subId, data=df)
reg_ncomments_2 <- felm(log_ncomments48 ~ log_cost | subId + weekId, data=df)
reg_ncomments_3 <- felm(log_ncomments48 ~ log_cost | subId + weekId + userId, data=df)

stargazer(
  reg_ncomments_0, reg_ncomments_1, reg_ncomments_2, reg_ncomments_3, 
  type="text",
  add.lines=list(
    c("Territory FE", "N", "Y", "Y", "Y"),
    c("Week FE",      "N", "N", "Y", "Y"),
    c("User FE",      "N", "N", "N", "Y")
  )
)
fe_df <- getfe(reg_ncomments_2)
fe_filename <- paste0(DATA_PATH, "/reg_ncomments_2.csv")
write.csv(fe_df, fe_filename, row.names=FALSE)
