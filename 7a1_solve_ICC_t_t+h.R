library(this.path)
library(data.table)
library(future.apply)
library(rootSolve)
library(haven)
library(dplyr)
library(lubridate) 

setwd(this.path::this.dir()) 
source("runmefirst.R")

# ==============================================================================
# 1. LOAD GDP DATA
# ==============================================================================
gdp_raw <- fread("../../data/Analyst/GDPA_PC1.csv")
gdp_raw[, Year := year(as.Date(observation_date))]
gdp_raw[, GDP_Growth := GDPA_PC1 / 100]
gdp_history <- gdp_raw[Year >= 1947, .(Year, GDP_Growth)]
setorder(gdp_history, Year)

# ==============================================================================
# 2. LOAD SAS DATA & CLEAN
# ==============================================================================
# We drop all the "next_" columns here because we will pair them dynamically later
dt_raw <- read_sas("../../data/Analyst/ibes_final.sas7bdat") %>%
  rename_all(tolower) %>%
  setDT()

# Keep only the base variables needed for time 't'
dt <- unique(dt_raw[, .(permno, statpers, price, eps1, eps2, ltg_decimal, payout_ratio, cfacshr)])
rm(dt_raw)

if (!("cfacshr" %in% names(dt))) {
  stop("CRITICAL ERROR: Split factor (cfacshr) missing.")
}

# Handle SAS Dates
if (is.numeric(dt$statpers)) {
  dt[, Date_Obj := as.Date(statpers, origin = "1960-01-01")]
} else {
  dt[, Date_Obj := as.Date(statpers)]
}

# Force dates to Month-End for perfect merging later
dt[, date := ceiling_date(Date_Obj, "month") - days(1)]

dt[, Year := year(date)]
dt[, Month := month(date)]
dt[, yyyymm := Year * 100 + Month]

# ==============================================================================
# 3. INTEGRATE DETAILED LTG (COAUTHOR'S DATA)
# ==============================================================================
print("Integrating Detailed LTG Consensus...")

det_ltg <- readRDS("tmp/analystjd/ltg_latest_by_month.RDS")

det_ltg_consensus <- det_ltg[, .(
  detailed_ltg = mean(ltg, na.rm = TRUE) / 100
), by = .(permno, yyyymm)]

dt <- merge(dt, det_ltg_consensus, by = c("permno", "yyyymm"), all.x = TRUE)
dt[, ltg_decimal := detailed_ltg] # OVERWRITE
dt[, detailed_ltg := NULL]

print(paste("Rows with valid Detailed LTG:", sum(!is.na(dt$ltg_decimal))))

# ==============================================================================
# 4. WINSORIZATION & G_SS MAP
# ==============================================================================
w_prob <- 0.005 
eps_cols <- c("eps1", "eps2") # Only need the current ones now

print("Cross-sectionally winsorizing EPS inputs by month...")
for (col in eps_cols) {
  dt[, (col) := {
    qs <- quantile(get(col), probs = c(w_prob, 1 - w_prob), na.rm = TRUE)
    pmin(pmax(get(col), qs[1]), qs[2])
  }, by = yyyymm] 
}

# G_SS Map
get_steady_state_g <- function(forecast_year) {
  valid_years <- gdp_history[Year < forecast_year, GDP_Growth]
  if(length(valid_years) == 0) return(0.06) 
  return(mean(valid_years))
}

unique_years <- unique(dt$Year)
g_ss_map <- sapply(unique_years, get_steady_state_g)
names(g_ss_map) <- unique_years 
dt[, G_SS_Current := g_ss_map[as.character(Year)]]

# ==============================================================================
# 5. SOLVER FUNCTION
# ==============================================================================
solve_icc_cdz <- function(eps1, eps2, ltg, payout_ratio, price, g_ss) {
  if (is.na(eps1) | is.na(eps2) | is.na(ltg) | is.na(price) | is.na(payout_ratio) | is.na(g_ss)) return(NA)
  if (price < 1) return(NA) 
  if (eps2 <= 0) return(NA) 
  
  if (payout_ratio > 1) payout_ratio <- 1.0
  if (payout_ratio < 0) payout_ratio <- 0.0
  if (ltg > 5.0) ltg <- 5.0
  if (ltg < -0.2) ltg <- -0.2 
  
  FE <- numeric(16)
  FE[1] <- eps1
  FE[2] <- eps2
  FE[3] <- eps2 * (1 + ltg)
  
  current_g <- ltg
  if (ltg <= 0.001) { decay <- 1 } else { decay <- exp(log(g_ss / ltg) / 13) }
  
  for (k in 4:16) {
    current_g <- current_g * decay
    FE[k] <- FE[k-1] * (1 + current_g)
  }
  
  icc_guess <- 0.12 
  max_iter <- 20
  
  for (iter in 1:max_iter) {
    safe_icc <- max(icc_guess, g_ss + 0.005)
    b_ss <- g_ss / safe_icc
    
    if (b_ss > 0.99) b_ss <- 0.99
    if (b_ss < 0) b_ss <- 0.0
    
    b_vec <- numeric(16)
    current_b <- 1 - payout_ratio
    b_vec[1] <- current_b
    b_vec[2] <- current_b
    step_size <- (b_vec[2] - b_ss) / 15
    for (k in 3:16) { b_vec[k] <- b_vec[k-1] - step_size }
    b_vec <- pmin(pmax(b_vec, 0), 1)
    
    val_func <- function(q) {
      if (q <= -0.99) return(1e9)
      discount_factors <- (1 + q)^(1:15)
      dividends <- FE[1:15] * (1 - b_vec[1:15])
      pv_explicit <- sum(dividends / discount_factors)
      tv <- FE[16] / q
      pv_tv <- tv / ((1 + q)^15)
      return ((pv_explicit + pv_tv) - price)
    }
    
    tryCatch({
      res <- uniroot(val_func, interval = c(0.001, 2.0), tol = 1e-6)
      new_icc <- res$root
      if (abs(new_icc - icc_guess) < 0.0001) return(new_icc) 
      icc_guess <- new_icc 
    }, error = function(e) { return(NA) })
  }
  return(NA) 
}

# ==============================================================================
# 6. EXECUTION
# ==============================================================================
nc <- parallel::detectCores()
plan(multisession, workers = nc - 1)

print("Calculating Base ICC for all firm-months...")
results_curr <- future_mapply(
  FUN = solve_icc_cdz,
  eps1 = dt$eps1,
  eps2 = dt$eps2,
  ltg = dt$ltg_decimal,
  payout_ratio = dt$payout_ratio,
  price = dt$price,
  g_ss = dt$G_SS_Current,
  future.seed = TRUE
)

plan(sequential)

dt[, ICC := results_curr]

# Drop rows where calculation failed to save space
dt_clean <- dt[!is.na(ICC)]

# Keep only the essentials for the decomposition step
dt_final <- dt_clean[, .(permno, date, yyyymm, price, eps1, eps2, ltg_decimal, payout_ratio, cfacshr, G_SS_Current, ICC)]

saveRDS(dt_final, paste0("../../data/Analyst/", "icc_base_single_rows.Rds"))
print("Done. Saved Base ICCs.")