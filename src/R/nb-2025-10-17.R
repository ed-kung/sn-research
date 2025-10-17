rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

ADDED_LINES <- list(
  c("Territory FE",       "N", "Y", "Y", "Y"),
  c("Week FE",            "N", "N", "Y", "Y"),
  c("Territory Owner FE", "N", "N", "N", "Y")
)

filename1 <- paste0(DATA_PATH, "/post_quality_analysis_data.csv")
filename2 <- paste0(DATA_PATH, "/post_quantity_analysis_data.csv")

# ---- Quantity Regressions

df2 <- read.csv(filename2)
df2$log_cost <- log(df2$posting_fee)
df2$log_posts <- log(1+df2$n_posts)

rquan0 <- felm(log_posts ~ log_cost, data=df2)
rquan1 <- felm(log_posts ~ log_cost | subId, data=df2)
rquan2 <- felm(log_posts ~ log_cost | subId + weekId, data=df2)
rquan3 <- felm(log_posts ~ log_cost | weekId + sub_subOwner_id, data=df2)

stargazer(
  rquan0, rquan1, rquan2, rquan3, type="text",
  add.lines = ADDED_LINES,
  title = "Post Quantity on Territory Cost"
)


# ---- Quality Regressions

df1 <- read.csv(filename1)
df1$log_sats48 <- log(1+df1$sats48)
df1$log_comments48 <- log(1+df1$comments48)
df1$log_cost <- log(df1$cost)

rzaps0 <- felm(log_sats48 ~ log_cost, data=df1)
rzaps1 <- felm(log_sats48 ~ log_cost | subId, data=df1)
rzaps2 <- felm(log_sats48 ~ log_cost | subId + weekId, data=df1)
rzaps3 <- felm(log_sats48 ~ log_cost | weekId + sub_subOwner_id, data=df1)

stargazer(
  rzaps0, rzaps1, rzaps2, rzaps3, type="text",
  add.lines = ADDED_LINES,
  title = "Post Quality (Zaps) on Territory Cost"
)

rcomm0 <- felm(log_comments48 ~ log_cost, data=df1)
rcomm1 <- felm(log_comments48 ~ log_cost | subId, data=df1)
rcomm2 <- felm(log_comments48 ~ log_cost | subId + weekId, data=df1)
rcomm3 <- felm(log_comments48 ~ log_cost | weekId + sub_subOwner_id, data=df1)

stargazer(
  rcomm0, rcomm1, rcomm2, rcomm3, type="text",
  add.lines = ADDED_LINES,
  title = "Post Quality (Comments) on Territory Cost"
)


# ---- Regress FE from quantity regression on btc price

btc_price_fe_reg <- function(maindf, regobj) {
  fedf <- getfe(regobj)
  fedf <- filter(fedf, fe=="weekId")
  fedf <- rename(fedf, weekId=idx)
  fedf$weekId <- as.integer(fedf$weekId)
  
  btc_df <- maindf %>% group_by(weekId) %>%
    summarize(btc_price = mean(btc_price)) %>% ungroup()
  
  regdf <- inner_join(btc_df, fedf, by=c("weekId"))
  regdf$log_price <- log(regdf$btc_price)
  
  reg <- lm(effect ~ log_price, data=regdf)
  return(reg)
}

rbtcquan <- btc_price_fe_reg(df2, rquan3)
rbtczaps <- btc_price_fe_reg(df1, rzaps3)
rbtccomm <- btc_price_fe_reg(df1, rcomm3)

stargazer(rbtcquan, rbtczaps, rbtccomm, type="text")

