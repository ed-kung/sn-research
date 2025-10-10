library(here)
library(lfe)
library(yaml)
library(stargazer)

LOCAL_CONFIG <- read_yaml("config.yaml.local")
DATA_PATH <- LOCAL_CONFIG["DATA_PATH"][[1]]

filename <- paste0(DATA_PATH, "/territory_by_week_panel.csv")

df <- read.csv(filename)

df$log_posts <- log(1+df$n_posts)
df$log_fee <- log(df$posting_fee)

reg <- felm(log_posts ~ log_fee | subId + weekId, data=df)

stargazer(reg, type="text")

fe_df <- getfe(reg)
fe_filename <- paste0(DATA_PATH, "/territory_by_week_fe.csv")
write.csv(fe_df, fe_filename, row.names=FALSE)