library(this.path)
library(data.table)
library(future.apply)
library(rootSolve)
library(haven)
library(dplyr)
library(lubridate) # Essential for date math

setwd(this.path::this.dir()) 
source("runmefirst.R")


gdp_raw <- fread("../../data/Analyst/GDPA_PC1.csv")
gdp_raw[, Year := year(as.Date(observation_date))]
gdp_raw[, GDP_Growth := GDPA_PC1 / 100]
gdp_history <- gdp_raw[Year >= 1947, .(Year, GDP_Growth)]
setorder(gdp_history, Year)

# Load SAS Data
dt <- read_sas("../../data/Analyst/ibes_final.sas7bdat") %>%
  rename_all(tolower) %>%
  setDT()
if (!("cfacshr" %in% names(dt)) | !("next_cfacshr" %in% names(dt))) {
  stop("CRITICAL ERROR: Split factors (cfacshr) are missing from the SAS data. 
       Check your SAS export.")
}

if (is.numeric(dt$statpers)) {
  dt[, Date_Obj := as.Date(statpers, origin = "1960-01-01")]
} else {
  dt[, Date_Obj := as.Date(statpers)]
}

# Define Years
dt[, Year := year(Date_Obj)]
dt[, Month := month(Date_Obj)]
dt[, yyyymm_curr := Year * 100 + Month]

# Define "Next Month" keys
dt[, Date_Next := Date_Obj %m+% months(1)]
dt[, Next_Year := year(Date_Next)]
dt[, yyyymm_next := Next_Year * 100 + month(Date_Next)]

# ==============================================================================
# 3. INTEGRATE DETAILED LTG (COAUTHOR'S DATA)
# ==============================================================================
print("Integrating Detailed LTG Consensus...")

# Load the detailed dataset saved from your coauthor's script
det_ltg <- readRDS("tmp/analystjd/ltg_latest_by_month.RDS")

# Aggregate across estimators to get the Firm-Month Consensus level
# Also, divide by 100 to convert from percentage (e.g., 15.0) to decimal (0.15)
det_ltg_consensus <- det_ltg[, .(
  detailed_ltg = mean(ltg, na.rm = TRUE) / 100
), by = .(permno, yyyymm)]

# Merge to replace Current LTG
dt <- merge(dt, det_ltg_consensus, 
            by.x = c("permno", "yyyymm_curr"), 
            by.y = c("permno", "yyyymm"), 
            all.x = TRUE)
dt[, ltg_decimal := detailed_ltg] # OVERWRITE old summary LTG
dt[, detailed_ltg := NULL]

# Merge to replace Next LTG
dt <- merge(dt, det_ltg_consensus, 
            by.x = c("permno", "yyyymm_next"), 
            by.y = c("permno", "yyyymm"), 
            all.x = TRUE)
dt[, next_ltg_decimal := detailed_ltg] # OVERWRITE old summary Next LTG
dt[, detailed_ltg := NULL]

w_prob <- 0.005 
eps_cols <- c("eps1", "eps2", "next_eps1", "next_eps2")

print("Cross-sectionally winsorizing EPS inputs by month...")

for (col in eps_cols) {
  dt[, (col) := {
    # Calculate bounds for THIS specific month (yyyymm_curr)
    qs <- quantile(get(col), probs = c(w_prob, 1 - w_prob), na.rm = TRUE)
    
    # Apply bounds
    pmin(pmax(get(col), qs[1]), qs[2])
  }, by = yyyymm_curr] # <--- Groups by month to handle inflation over time
}


# Check how many are missing Detailed LTG 
# (The solver will naturally return NA for these, which is what you want)
print(paste("Rows with valid Detailed LTG:", sum(!is.na(dt$ltg_decimal))))

# Function for Steady State Growth
get_steady_state_g <- function(forecast_year) {
  valid_years <- gdp_history[Year < forecast_year, GDP_Growth]
  if(length(valid_years) == 0) return(0.06) 
  return(mean(valid_years))
}

# --- MAP G_SS FOR CURRENT YEAR ---
unique_years <- unique(dt$Year)
g_ss_map <- sapply(unique_years, get_steady_state_g)
names(g_ss_map) <- unique_years # Ensure names are set for lookup
dt[, G_SS_Current := g_ss_map[as.character(Year)]]

# --- MAP G_SS FOR NEXT YEAR (CORRECTED) ---
# Logic: If month is Dec, next month is Jan of Year+1.
dt[, Month := month(Date_Obj)]
dt[, Next_Year := ifelse(Month == 12, Year + 1, Year)]

# Expand Map for Next Years (in case T+1 extends beyond current data range)
all_years_needed <- unique(c(dt$Year, dt$Next_Year))
g_ss_map_full <- sapply(all_years_needed, get_steady_state_g)
names(g_ss_map_full) <- all_years_needed

# Assign
dt[, G_SS_Next := g_ss_map_full[as.character(Next_Year)]]

# ==============================================================================
# 2. SOLVER FUNCTION (FIXED VALIDATION)
# ==============================================================================

solve_icc_cdz <- function(eps1, eps2, ltg, payout_ratio, price, g_ss) {
  
  # --- VALIDATION FIX: Check g_ss here to prevent crash ---
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
    
    # Validation prevents b_ss being NA here
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
# 3. EXECUTION
# ==============================================================================

nc <- parallel::detectCores()
plan(multisession, workers = nc - 1)

print("1. Calculating Current ICC...")
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

print("2. Calculating Next Month ICC...")
results_next <- future_mapply(
  FUN = solve_icc_cdz,
  eps1 = dt$next_eps1,
  eps2 = dt$next_eps2,
  ltg = dt$next_ltg_decimal,
  payout_ratio = dt$next_payout_ratio, 
  price = dt$next_price,
  g_ss = dt$G_SS_Next,
  future.seed = TRUE
)

plan(sequential)

dt[, ICC := results_curr]
dt[, Next_ICC := results_next]

# ==============================================================================
# 4. SAVE
# ==============================================================================
saveRDS(dt, file = paste0("../../data/Analyst/", "icc_pairs.Rds"))
print("Done.")