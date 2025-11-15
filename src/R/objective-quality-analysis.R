rm(list=ls())

library(lfe)
library(yaml)
library(stargazer)
library(dplyr)
library(arrow)

LOCAL_CONFIG <- read_yaml("../../config.yaml.local")
LOCAL_PATH <- LOCAL_CONFIG["LOCAL_PATH"][[1]]
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]
TABLE_TYPE <- "text"

ADDED_LINES <- list(
  c("Territory FE",       "N", "Y", "Y", "Y"),
  c("Week FE",            "N", "N", "Y", "Y"),
  c("Territory Owner FE", "N", "N", "N", "Y")
)

qual_file <- paste0(DATA_PATH, "/objective_quality_analysis.parquet")

qual_df = read_parquet(qual_file)

qual_df$log_cost <- log(1 + qual_df$cost)


# ---- Quality Regression (Objective Measure)

rzaps0 <- felm(log_sats48_pred ~ log_cost, data=qual_df)
rzaps1 <- felm(log_sats48_pred ~ log_cost | subId, data=qual_df)
rzaps2 <- felm(log_sats48_pred ~ log_cost | subId + weekId, data=qual_df)
rzaps3 <- felm(log_sats48_pred ~ log_cost | weekId + sub_subOwner_id, data=qual_df)

zaps_tbl <- stargazer(
  rzaps0, rzaps1, rzaps2, rzaps3, type=TABLE_TYPE,
  covariate.labels = "log(Posting Cost)",
  add.lines = ADDED_LINES
)

if (TABLE_TYPE=="latex") {
  outlines <- zaps_tbl[c(15:25, 29:30)]
  outfile <- paste0(LOCAL_PATH, "/results/tbl_objqual_cost_reg.tex")
  writeLines(outlines, outfile)
}

outfile <- paste0(DATA_PATH, "/objqual_cost_reg.csv")
write.csv(as.data.frame(coef(rzaps3)), outfile)

