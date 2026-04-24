library(this.path)
library(data.table)
library(future.apply)
library(dplyr)

setwd(this.path::this.dir()) 
source("runmefirst.R") 

# Load the Pair-wise ICC data (Generated in previous step)
dt <- readRDS("../../data/Analyst/icc_pairs.Rds")

# Filter: We can only decompose if we successfully solved ICC for both t and t+1
dt_calc <- dt[!is.na(ICC) & !is.na(Next_ICC)]


dt_calc[, Split_Adj := ifelse(is.na(next_cfacshr) | is.na(cfacshr) | cfacshr == 0, 
                              1.0, 
                              next_cfacshr / cfacshr)]


print(paste("Observations available for decomposition:", nrow(dt_calc)))

# ==============================================================================
# 2. DEFINE VALUATION FUNCTION (The "Price Calculator")
# ==============================================================================
# This function applies the CDZ valuation logic to return a Price (PV) 
# given a specific set of Cash Flows (c) and a specific Discount Rate (q).

get_implied_price_cdz <- function(eps1, eps2, ltg, payout_ratio, g_ss, q) {
  
  # --- Safety Checks ---
  if (is.na(q) | is.na(eps1) | is.na(g_ss)) return(NA)
  
  # 1. Infinite Value Trap
  # If the discount rate q is less than or equal to the steady state growth g_ss,
  # the terminal value (and thus Price) mathematically approaches infinity.
  # We must return NA for these cases.
  if (q <= g_ss + 0.0001) return(NA)
  if (q <= 0) return(NA)
  
  # --- Clean Inputs (Same bounds as Solver) ---
  if (payout_ratio > 1) payout_ratio <- 1.0
  if (payout_ratio < 0) payout_ratio <- 0.0
  if (ltg > 5.0) ltg <- 5.0
  if (ltg < -0.2) ltg <- -0.2
  
  # --- A. Build Earnings Vector (FE) ---
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
  
  # --- B. Build Plowback Vector (b) ---
  # Note: Plowback depends on q because b_ss = g_ss / q
  
  b_ss <- g_ss / q
  if (b_ss > 0.99) b_ss <- 0.99
  if (b_ss < 0) b_ss <- 0.0
  
  b_vec <- numeric(16)
  current_b <- 1 - payout_ratio
  b_vec[1] <- current_b
  b_vec[2] <- current_b
  
  step_size <- (b_vec[2] - b_ss) / 15
  for (k in 3:16) {
    b_vec[k] <- b_vec[k-1] - step_size
  }
  b_vec <- pmin(pmax(b_vec, 0), 1)
  
  # --- C. Calculate Present Value ---
  discount_factors <- (1 + q)^(1:15)
  dividends <- FE[1:15] * (1 - b_vec[1:15])
  
  pv_explicit <- sum(dividends / discount_factors)
  
  # Terminal Value (Year 16 Earnings capitalized by q)
  # Discounted back 15 years
  tv <- FE[16] / q
  pv_tv <- tv / ((1 + q)^15)
  
  return(pv_explicit + pv_tv)
}

# ==============================================================================
# 3. CALCULATE CROSS-TERMS
# ==============================================================================
# We need to calculate the two hypothetical prices required for Eq (4) and (5).

# Setup Parallel
nc <- parallel::detectCores()
plan(multisession, workers = nc - 1)

print("Calculating Cross-Term 1: f(c_next, q_curr)...")
# Logic: Use NEXT month's Cash Flows, but CURRENT month's ICC
raw_price_next_curr <- future_mapply(
  FUN = get_implied_price_cdz,
  eps1 = dt_calc$next_eps1,
  eps2 = dt_calc$next_eps2,
  ltg = dt_calc$next_ltg_decimal,
  payout_ratio = dt_calc$next_payout_ratio,
  g_ss = dt_calc$G_SS_Next,
  q = dt_calc$ICC, 
  future.seed = TRUE
)
# APPLY ADJUSTMENT HERE
dt_calc[, P_cNext_qCurr := raw_price_next_curr * Split_Adj] 


print("Calculating Cross-Term 2: f(c_curr, q_next)...")
# Note: 'eps' inputs are pre-split (current). Result is pre-split.
# No adjustment needed here.
dt_calc[, P_cCurr_qNext := future_mapply(
  FUN = get_implied_price_cdz,
  eps1 = dt_calc$eps1,
  eps2 = dt_calc$eps2,
  ltg = dt_calc$ltg_decimal,
  payout_ratio = dt_calc$payout_ratio,
  g_ss = dt_calc$G_SS_Current,
  q = dt_calc$Next_ICC, 
  future.seed = TRUE
)]
#dt_calc[, P_cCurr_qNext := NULL]
plan(sequential)

# ==============================================================================
# 4. APPLY DECOMPOSITION (WITH SPLIT ADJUSTMENT)
# ==============================================================================

dt_final <- dt_calc[!is.na(P_cNext_qCurr) & !is.na(P_cCurr_qNext)]

# Define Adjusted Next Price
# If stock splits 2:1, P_next drops by half. We multiply by 2 to compare to P_curr.
dt_final[, Price_Next_Adj := next_price * Split_Adj]

# --- Equation 3: Realized Capital Gain Return ---
dt_final[, Retx := (Price_Next_Adj - price) / price]

# --- Equation 4: Cash Flow News ---
# Numerator 1: (Price_Next_Adj) - P_cCurr_qNext
# Numerator 2: P_cNext_qCurr    - price
dt_final[, CF_News := 0.5 * ( (Price_Next_Adj - P_cCurr_qNext)/price + 
                                (P_cNext_qCurr - price)/price ) ]

# --- Equation 5: Discount Rate News ---
# Numerator 1: P_cCurr_qNext    - price
# Numerator 2: (Price_Next_Adj) - P_cNext_qCurr
dt_final[, DR_News := 0.5 * ( (P_cCurr_qNext - price)/price + 
                                (Price_Next_Adj - P_cNext_qCurr)/price ) ]

# ==============================================================================
# 5. VALIDATION AND SAVE
# ==============================================================================

dt_final[, Check_Diff := Retx - (CF_News + DR_News)]

print("--- Validation Summary (Should be ~0) ---")
print(summary(dt_final$Check_Diff))

print("--- News Summary ---")
print(summary(dt_final[, .(Retx, CF_News, DR_News)]))

# Final Save (Aligning Date to Month End)
# Ensure 'statpers' is converted to Date object properly before math
dt_save <- dt_final %>%
  transmute(
    permno, 
    date = ceiling_date(as.Date(statpers) %m+% months(1), "month") - days(1),
    ltg = next_ltg_decimal,
    cf = CF_News, 
    dr = DR_News, 
    sum_news = cf + dr
  ) %>%
  setDT()

# Save the decomposed data

saveRDS(dt_save, "../../data/Analyst/news_decomposition_monthly_non_winsored.Rds")


setDT(dt_save)

# 1. Define the winsorization threshold (1% and 99% are standard for noisy components)
w_prob <- 0.01 

dt_save[, c("cf", "dr") := {
  
  # A. Cross-Sectional Tail Winsorization (1% and 99%)
  bounds <- quantile(cf, probs = c(w_prob, 1 - w_prob), na.rm = TRUE)
  cf_capped <- pmin(pmax(cf, bounds[1]), bounds[2])
  
  # B. Apply the Economic Floor (-1)
  # If the capped CF is still somehow below -1, force it to -1
  cf_capped <- pmax(cf_capped, -1.0)
  
  # C. Recalculate DR to preserve the identity
  dr_capped <- sum_news - cf_capped
  
  # D. Ensure DR doesn't violate the -1 floor
  # If DR is < -1, we floor DR at -1, and push the residual back to CF
  # (This happens if CF was massively positive)
  cf_final <- ifelse(dr_capped < -1.0, sum_news - (-1.0), cf_capped)
  dr_final <- ifelse(dr_capped < -1.0, -1.0, dr_capped)
  
  .(cf_final, dr_final)
  
}, by = date] # Group by date for the cross-sectional percentiles

# Verification
print("After Bounding and Identity-Preservation:")
print(paste("CF Range:", paste(round(range(dt_save$cf, na.rm=T), 2), collapse=" to ")))
print(paste("DR Range:", paste(round(range(dt_save$dr, na.rm=T), 2), collapse=" to ")))

max_error <- max(abs(dt_save$sum_news - (dt_save$cf + dt_save$dr)), na.rm=TRUE)
print(paste("Identity Error (Must be ~0):", max_error))

saveRDS(dt_save, "../../data/Analyst/news_decomposition_monthly.Rds")