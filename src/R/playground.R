rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

filename <- paste0(DATA_PATH, "/user_analysis_data.csv")

df <- read.csv(filename)

df$inactive <- (df$weeks_since_last_activity>=1) & (df$length_of_inactivity>=4)
df$became_inactive <- (df$weeks_since_last_activity==1) & (df$length_of_inactivity>=4)
df$unprofitable <- (df$rolling_profit1 < 0)
#df$log_rolling_profit <- log(1+df$rolling_profit1)
df$logprice <- log(df$btc_price)
df$pgrowth <- df$mom_growth
df$unprofitable_X_pgrowth <- df$unprofitable * df$pgrowth

regdf <- df %>%
  filter(!inactive | became_inactive) %>%
  filter(!is.na(rolling_profit1))

r0 <- glm(became_inactive ~ unprofitable, data=regdf, family = binomial(link = "logit"))
r1 <- glm(became_inactive ~ unprofitable + pgrowth, data=regdf, family = binomial(link = "logit"))
r2 <- glm(became_inactive ~ unprofitable + pgrowth + unprofitable_X_pgrowth, data=regdf, family = binomial(link = "logit"))

stargazer(
  r0, r1, r2, type="text"
)

r0 <- felm(became_inactive ~ unprofitable, data=regdf)
r1 <- felm(became_inactive ~ unprofitable + pgrowth, data=regdf)
r2 <- felm(became_inactive ~ unprofitable + pgrowth + unprofitable_X_pgrowth, data=regdf)
r3 <- felm(became_inactive ~ unprofitable + unprofitable_X_pgrowth | week, data=regdf)

stargazer(
  r0, r1, r2, r3, type="text"
)

