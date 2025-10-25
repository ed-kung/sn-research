rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
LOCAL_PATH <- LOCAL_CONFIG["LOCAL_PATH"][[1]]
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
df$logitems <- log(1 + df$rolling_items)

regdf <- df %>%
  filter(!inactive | became_inactive) %>%
  filter(!is.na(rolling_profit1))

r0 <- glm(became_inactive ~ unprofitable + pgrowth, data=regdf, family = binomial(link = "logit"))
r1 <- glm(became_inactive ~ unprofitable + pgrowth + unprofitable_X_pgrowth, data=regdf, family = binomial(link = "logit"))
r2 <- glm(became_inactive ~ unprofitable + pgrowth + unprofitable_X_pgrowth + logitems, data=regdf, family = binomial(link = "logit"))
r3 <- felm(became_inactive ~ unprofitable + unprofitable_X_pgrowth + logitems | week, data=regdf)

tbl <- stargazer(
  r0, r1, r2, r3, type="latex",
  covariate.labels = c(
    "Unprofitable last 8 weeks",
    "MoM BTC price growth",
    "Unprofitable X price growth",
    "log(Items) last 8 weeks"
  ),
  add.lines = list(
    c("Model", "Logit", "Logit", "Logit", "LPM"),
    c("Week FE", "N", "N", "N", "Y")
  ),
  model.names = FALSE
)

outfile <- paste0(LOCAL_PATH, "/results/tbl_exit_reg.tex")
writeLines(tbl[c(15:33, 39:40)], outfile)



