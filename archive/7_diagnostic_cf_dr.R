library(this.path)
library(data.table)
library(future.apply)
library(rootSolve)
library(haven)
library(dplyr)
library(lubridate) # Essential for date math

setwd(this.path::this.dir()) 
source("runmefirst.R")



dt_icc <- readRDS("../../data/Analyst/icc_pairs.Rds")

# Filter for Quarter-Ends (March, June, Sept, Dec) as per paper convention
dt_icc[, Month := month(as.Date(statpers, origin="1960-01-01"))]
dt_icc[, Year  := year(as.Date(statpers, origin="1960-01-01"))]
dt_qtr <- dt_icc[Month %in% c(3, 6, 9, 12) & !is.na(ICC)]

# Calculate Statistics
qtr_stats <- dt_qtr[, .(
  N_Firms = .N,
  ICC_Med = median(ICC, na.rm=TRUE),
  ICC_Q1  = quantile(ICC, 0.25, na.rm=TRUE),
  ICC_Q3  = quantile(ICC, 0.75, na.rm=TRUE),
  ICC_Std = sd(ICC, na.rm=TRUE)
), by = .(Year, Month)]

# Annual Averages
table1_df <- qtr_stats[, .(
  N_Firms = round(mean(N_Firms), 0),
  ICC_Med = round(mean(ICC_Med) * 100, 1), # Convert to %
  ICC_Q1  = round(mean(ICC_Q1) * 100, 1),
  ICC_Q3  = round(mean(ICC_Q3) * 100, 1),
  ICC_Std = round(mean(ICC_Std) * 100, 1)
), by = Year]

setorder(table1_df, Year)

# Add Paper's Target Values for comparison (Manually extracted from CDZ 2013)
# We join this to your results for the final report
targets <- data.table(
  Year = c(1985, 1990, 2000, 2008, 2010),
  Paper_Med = c(15.8, 14.8, 15.5, 13.2, 11.5)
)
table1_final <- merge(table1_df, targets, by="Year", all=FALSE)

# Save
saveRDS(table1_final, "output_table1.Rds")





dt_news <- readRDS("../../data/Analyst/news_decomposition_monthly.Rds")
setorder(dt_news, permno, date)

# Define Horizons to check (in Months)
# CDZ Table 2 checks 1Q (3m), 1Y (12m), 2Y (24m), etc.
horizons <- c(1, 3, 6, 12, 24, 36)
results_list <- list()

for (h in horizons) {
  
  # Calculate Cumulative Sums for this horizon
  # We use a temporary data.table to avoid bloating the main one
  temp_dt <- dt_news[, .(
    permno, date,
    # Rolling Sum of CF News
    CF_Cum = frollsum(cf, h, fill=NA, algo="exact"),
    # Rolling Sum of Total News (Implied Return)
    Ret_Cum = frollsum(cf + dr, h, fill=NA, algo="exact")
  ), by = permno]
  
  # Remove NAs created by rolling window
  temp_dt <- temp_dt[!is.na(Ret_Cum)]
  
  # Run Regression: CF ~ Total_Return
  # Slope = Var(CF) + Cov(CF, DR) / Var(Ret)
  if (nrow(temp_dt) > 1000) {
    model <- lm(CF_Cum ~ Ret_Cum, data = temp_dt)
    beta_cf <- coef(model)["Ret_Cum"]
    beta_dr <- 1 - beta_cf # Identity holds
    
    # Store Result
    results_list[[paste0(h)]] <- data.table(
      Horizon_Months = h,
      Beta_CF = round(beta_cf, 3),
      Beta_DR = round(beta_dr, 3),
      Obs = nrow(temp_dt)
    )
  }
}

table2_df <- rbindlist(results_list)

# Add CDZ Paper approximate benchmarks for context (Firm Level)
# Based on Table 2 Panel B of the paper
benchmarks <- data.table(
  Horizon_Months = c(3, 12, 24),
  Paper_Beta_CF = c(0.19, 0.48, 0.63)
)
table2_final <- merge(table2_df, benchmarks, by="Horizon_Months", all.x=TRUE)

saveRDS(table2_final, "output_table2.Rds")


dt_news[, Ret_12m := frollsum(cf + dr, 12, fill=NA), by=permno]
dt_news[, CF_12m := frollsum(cf, 12, fill=NA), by=permno]

set.seed(123)
plot_sample <- dt_news[!is.na(Ret_12m) & abs(Ret_12m) < 1.0] # Remove outliers
plot_sample <- plot_sample[sample(.N, 5000)] # Sample 5000 points

saveRDS(plot_sample, "output_plot_sample.Rds")