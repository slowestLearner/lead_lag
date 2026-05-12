library(this.path)
library(data.table)
library(future.apply)
library(rootSolve)
library(haven)
library(dplyr)
library(lubridate) # Essential for date math

setwd(this.path::this.dir()) 
source("runmefirst.R")
dt <- readRDS("../../data/Analyst/icc_pairs.Rds")

# 2. Filter and Prepare
# The paper reports "average quarterly statistics".
# We usually take snapshots at March, June, Sept, Dec.
dt[, Month := month(as.Date(statpers, origin="1960-01-01"))]
dt[, Year  := year(as.Date(statpers, origin="1960-01-01"))]

# Keep only Quarter-Ends and Valid ICCs
dt_qtr <- dt[Month %in% c(3, 6, 9, 12) & !is.na(ICC)]

# 3. Calculate Quarterly Stats First
# (Calculate the Median across firms for EACH quarter)
qtr_stats <- dt_qtr[, .(
  N_Firms = .N,
  ICC_Q1  = quantile(ICC, 0.25, na.rm=TRUE),
  ICC_Med = median(ICC, na.rm=TRUE),
  ICC_Q3  = quantile(ICC, 0.75, na.rm=TRUE),
  ICC_Std = sd(ICC, na.rm=TRUE)
), by = .(Year, Month)]

# 4. Calculate Yearly Averages (Average of the 4 quarters)
table1_rep <- qtr_stats[, .(
  Avg_N_Firms = round(mean(N_Firms), 0),
  ICC_Q1      = round(mean(ICC_Q1) * 100, 1),  # Convert to %
  ICC_Med     = round(mean(ICC_Med) * 100, 1),
  ICC_Q3      = round(mean(ICC_Q3) * 100, 1),
  ICC_Std     = round(mean(ICC_Std) * 100, 1)
), by = Year]

setorder(table1_rep, Year)

# 5. Print Comparison for Key Years
# These numbers are manually typed from Chen, Da, Zhao (2013) Table 1
paper_vals <- data.table(
  Year = c(1985, 1990, 2000, 2008, 2010),
  Paper_Med = c(15.8, 14.8, 15.5, 13.2, 11.5)
)

print("--- Your Replication Results ---")
print(table1_rep[Year %in% c(1985, 1990, 2000, 2008, 2010)])

print("--- Original Paper Values (Target) ---")
print(paper_vals)




#####################################
dt <- readRDS("../../data/Analyst/news_decomposition_monthly.Rds")

# 2. Basic Data Prep
setorder(dt, permno, date)

# 3. Create Cumulative Returns (12-Month Horizon)
# We need to sum the News components over 12 months to get annual news.
# Note: For strict identity, Sum(News) = Sum(Ret).
dt[, `:=`(
  CF_12m = frollsum(cf, 12, fill = NA, algo = "exact"),
  DR_12m = frollsum(dr, 12, fill = NA, algo = "exact"),
  Ret_12m = frollsum(cf + dr, 12, fill = NA, algo = "exact") # Implied Cumulative Return
), by = permno]

# 4. Run the Validation Regressions

# --- A. Monthly Horizon (1 Month) ---
# Regress CF_News on Total_Return
model_1m <- feols(cf ~ sum_news, data = dt)
beta_cf_1m <- coef(model_1m)["sum_news"]

# --- B. Annual Horizon (12 Months) ---
# Regress CF_12m on Ret_12m
model_12m <- feols(CF_12m ~ Ret_12m, data = dt[!is.na(Ret_12m)])
beta_cf_12m <- coef(model_12m)["Ret_12m"]

# 5. Print Results
cat("\n==============================================\n")
cat("VALIDATION CHECK: IMPORTANCE OF CASH FLOW NEWS\n")
cat("==============================================\n")
cat(sprintf("1-Month Horizon (Target: < 0.15): %.3f\n", beta_cf_1m))
cat(sprintf("1-Year  Horizon (Target: ~ 0.48): %.3f\n", beta_cf_12m))
cat("==============================================\n")
cat("Interpretation:\n")
cat("If 1-Year Beta >> 1-Month Beta, your replication is successful.\n")
cat("This confirms that fundamentals (CF) drive long-term returns.\n")


