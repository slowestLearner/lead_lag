library(this.path)
library(data.table)
library(future.apply)
library(rootSolve)
library(haven)
library(dplyr)
library(lubridate) # Essential for date math

setwd(this.path::this.dir()) 
source("runmefirst.R")

# Load SAS Data
dt <- read_sas("../../data/Analyst/det_ltg_consensus.sas7bdat") %>%
  rename_all(tolower) %>%
  dplyr::select(permno, ltg, statpers) %>%
  setDT()

###############check the data########





#############





dt[, date_1st := floor_date(as.Date(statpers), "month")]

skeleton <- dt[, .(
  date_1st = seq.Date(from = min(date_1st), to = max(date_1st), by = "month")
), by = permno]


dt_full <- merge(skeleton, dt, by = c("permno", "date_1st"), all.x = TRUE)

setorder(dt_full, permno, date_1st)

dt_full[, ltg_filled := nafill(ltg, type = "locf"), by = permno]

dt_full[, ltg_chg := ltg_filled - shift(ltg_filled, n = 1, type = "lag"), by = permno]

dt_full[, statpers_final := ceiling_date(date_1st, "month") - days(1)]

# Select final columns
final_data <- dt_full[!is.na(ltg_chg), .(permno, statpers = statpers_final, ltg_filled, ltg_chg)]

saveRDS(final_data[,.(permno, date = statpers,
                      ltg_chg)], file = paste0("../../data/Analyst/", "ltg_chg_detail.Rds"))
