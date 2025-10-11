rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

filename <- paste0(DATA_PATH, "/posts_quality_regs.csv")

df <- read.csv(filename)

df$log_sats48 <- log(1+df$sats48)
df$log_cost <- log(df$cost)

reg0 <- felm(log_sats48 ~ log_cost, data=df)
reg1 <- felm(log_sats48 ~ log_cost | subId, data=df)
reg2 <- felm(log_sats48 ~ log_cost | subId + weekId, data=df)
reg3 <- felm(log_sats48 ~ log_cost | subId + weekId + userId, data=df)

stargazer(reg0, reg1, reg2, reg3, type="text")

fe_df <- getfe(reg3)
fe_filename <- paste0(DATA_PATH, "/posts_quality_regs_fe.csv")
write.csv(fe_df, fe_filename, row.names=FALSE)
