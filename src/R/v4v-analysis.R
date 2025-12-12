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

ADDED_LINES <- list(
  c("User FE",       "N", "N", "Y", "Y"),
  c("Week FE",            "N", "N", "N", "Y")
)


filename <- paste0(DATA_PATH, "/v4v_analysis_data.parquet")

df <- read_parquet(filename)

df$log_prior_hi_quality_zaps <- log(1+df$prior_zaps_from_hi_quality)
df$log_prior_zaps <- log(1+df$prior_zaps)
df$hi_quality_share <- df$prior_zaps_from_hi_quality / (1+df$prior_zaps)

r1 <- felm(hi_quality ~ hi_quality_share, data=df)
r2 <- felm(hi_quality ~ hi_quality_share + log_prior_zaps, data=df)
r3 <- felm(hi_quality ~ hi_quality_share + log_prior_zaps | userId, data=df)
r4 <- felm(hi_quality ~ hi_quality_share + log_prior_zaps | userId + weekId, data=df)

out_tbl <- stargazer(
  r1, r2, r3, r4, type=TABLE_TYPE,
  covariate.labels = c("High quality share of prior zaps", "log(Prior zaps from all posts)"),
  add.lines = ADDED_LINES
)

if (TABLE_TYPE=="latex") {
  outlines <- out_tbl[c(15:27, 31:32)]
  outfile <- paste0(LOCAL_PATH, "/results/tbl_v4v_learning.tex")
  writeLines(outlines, outfile)
}

outfile <- paste0(DATA_PATH, "/v4v_learning_coefs.csv")
write.csv(as.data.frame(coef(r4)), outfile)



