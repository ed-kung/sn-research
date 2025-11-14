rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)
library(arrow)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
LOCAL_PATH <- LOCAL_CONFIG["LOCAL_PATH"][[1]]
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

ADDED_LINES <- list(
  c("Territory FE",       "N", "Y", "Y", "Y"),
  c("Week FE",            "N", "N", "Y", "Y"),
  c("Territory Owner FE", "N", "N", "N", "Y")
)

qual_file <- paste0(DATA_PATH, "/post_quality_analysis_data.parquet")
quant_file <- paste0(DATA_PATH, "/post_quantity_analysis_data.parquet")

qual_df = read_parquet(qual_file)
quant_df = read_parquet(quant_file)



# ---- Quantity Regressions


df2 <- read.csv(filename2)
df2$log_cost <- log(df2$posting_fee)
df2$log_posts <- log(1+df2$n_posts)

rquan0 <- felm(log_posts ~ log_cost, data=df2)
rquan1 <- felm(log_posts ~ log_cost | subId, data=df2)
rquan2 <- felm(log_posts ~ log_cost | subId + weekId, data=df2)
rquan3 <- felm(log_posts ~ log_cost | weekId + sub_subOwner_id, data=df2)

quan_tbl <- stargazer(
  rquan0, rquan1, rquan2, rquan3, type="latex",
  covariate.labels = "log(Posting Cost)",
  add.lines = ADDED_LINES
)

outfile <- paste0(LOCAL_PATH, "/results/tbl_posts_cost_reg.tex")
writeLines(quan_tbl[c(15:25, 29:30)], outfile)

outfile <- paste0(DATA_PATH, "/posts_cost_reg.csv")
write.csv(as.data.frame(coef(rquan3)), outfile)


# ---- Quality Regression (Zaps)

df1 <- read.csv(filename1)
df1$log_sats48 <- log(1+df1$sats48)
df1$log_comments48 <- log(1+df1$comments48)
df1$log_cost <- log(df1$cost)

rzaps0 <- felm(log_sats48 ~ log_cost, data=df1)
rzaps1 <- felm(log_sats48 ~ log_cost | subId, data=df1)
rzaps2 <- felm(log_sats48 ~ log_cost | subId + weekId, data=df1)
rzaps3 <- felm(log_sats48 ~ log_cost | weekId + sub_subOwner_id, data=df1)

zaps_tbl <- stargazer(
  rzaps0, rzaps1, rzaps2, rzaps3, type="latex",
  covariate.labels = "log(Posting Cost)",
  add.lines = ADDED_LINES
)

outfile <- paste0(LOCAL_PATH, "/results/tbl_sats48_cost_reg.tex")
writeLines(zaps_tbl[c(15:25, 29:30)], outfile)

outfile <- paste0(DATA_PATH, "/sats48_cost_reg.csv")
write.csv(as.data.frame(coef(rzaps3)), outfile)



# ---- Quality Regression (Comments)

rcomm0 <- felm(log_comments48 ~ log_cost, data=df1)
rcomm1 <- felm(log_comments48 ~ log_cost | subId, data=df1)
rcomm2 <- felm(log_comments48 ~ log_cost | subId + weekId, data=df1)
rcomm3 <- felm(log_comments48 ~ log_cost | weekId + sub_subOwner_id, data=df1)

comm_tbl <- stargazer(
  rcomm0, rcomm1, rcomm2, rcomm3, type="latex",
  covariate.labels = "log(Posting Cost)",
  add.lines = ADDED_LINES
)
outfile <- paste0(LOCAL_PATH, "/results/tbl_comments48_cost_reg.tex")
writeLines(comm_tbl[c(15:25, 29:30)], outfile)

outfile <- paste0(DATA_PATH, "/comments48_cost_reg.csv")
write.csv(as.data.frame(coef(rcomm3)), outfile)
