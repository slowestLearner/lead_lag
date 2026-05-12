library(this.path)
library(data.table)
library(future.apply)
library(lubridate)
library(dplyr)

setwd(this.path::this.dir()) 
source("runmefirst.R") 

# ==============================================================================
# 1. LOAD BASE ICC DATA
# ==============================================================================
cat("Loading Base ICC Data...\n")
dt_base <- readRDS("../../data/Analyst/icc_base_single_rows.Rds")
setDT(dt_base)

# Ensure dates are proper Date objects and forced to Month-End for perfect merging
if(!inherits(dt_base$date, "Date")) {
  dt_base[, date := as.Date(date)]
}
dt_base[, date := ceiling_date(date, "month") - days(1)]
setkey(dt_base, permno, date)

# ==============================================================================
# 2. VALUATION FUNCTION (The Price Calculator)
# ==============================================================================
get_implied_price_cdz <- function(eps1, eps2, ltg, payout_ratio, g_ss, q) {
  
  if (is.na(q) | is.na(eps1) | is.na(g_ss)) return(NA_real_)
  if (q <= g_ss + 0.0001) return(NA_real_)
  if (q <= 0) return(NA_real_)
  
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
  
  discount_factors <- (1 + q)^(1:15)
  dividends <- FE[1:15] * (1 - b_vec[1:15])
  
  pv_explicit <- sum(dividends / discount_factors)
  tv <- FE[16] / q
  pv_tv <- tv / ((1 + q)^15)
  
  return(pv_explicit + pv_tv)
}

# ==============================================================================
# 3. SETUP PARALLEL BACKEND & OUTPUT DIRECTORY
# ==============================================================================
nc <- parallel::detectCores()
plan(multisession, workers = nc - 1)

out_dir <- "../../data/Analyst/horizons_cf_dr/"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

horizons <- 1:180

# ==============================================================================
# 4. HORIZON LOOP (t -> t+h)
# ==============================================================================
for (h in horizons) {
  
  file_out <- paste0(out_dir, "hor_", h, ".RDS")
  if (file.exists(file_out)) {
    cat(paste("Horizon", h, "already exists. Skipping...\n"))
    next
  }
  
  cat(paste("\n--- Processing Horizon h =", h, "---\n"))
  
  # A. DYNAMIC PAIRING
  dt_future <- copy(dt_base)
  # Shift future date backwards by h months to align with Time t
  dt_future[, target_date := ceiling_date(date %m-% months(h), "month") - days(1)]
  
  # Inner join: matches t with t+h
  dt_calc <- dt_future[dt_base, 
                       .(permno, yyyymm = i.yyyymm, date_t = i.date, date_h = x.date,
                         # Current (t)
                         price = i.price, eps1 = i.eps1, eps2 = i.eps2, 
                         ltg_decimal = i.ltg_decimal, payout_ratio = i.payout_ratio,
                         cfacshr = i.cfacshr, G_SS_Current = i.G_SS_Current, ICC = i.ICC,
                         # Future (t+h)
                         next_price = x.price, next_eps1 = x.eps1, next_eps2 = x.eps2, 
                         next_ltg_decimal = x.ltg_decimal, next_payout_ratio = x.payout_ratio,
                         next_cfacshr = x.cfacshr, G_SS_Next = x.G_SS_Current, Next_ICC = x.ICC
                       ), 
                       on = .(permno, target_date = date), 
                       nomatch = 0] 
  
  if (nrow(dt_calc) == 0) {
    cat("No valid pairs found for this horizon. Skipping.\n")
    next
  }
  
  # Split Adjustment (Future Shares / Current Shares)
  dt_calc[, Split_Adj := ifelse(is.na(next_cfacshr) | is.na(cfacshr) | cfacshr == 0, 
                                1.0, next_cfacshr / cfacshr)]
  
  # B. CALCULATE CROSS TERMS
  # Cross-Term 1: Future CF, Current Rate
  # Note: Future CF implies using G_SS from t+h (G_SS_Next)
  cat("   Calculating f(c_t+h, q_t)...\n")
  raw_price_next_curr <- future_mapply(
    FUN = get_implied_price_cdz,
    eps1 = dt_calc$next_eps1, eps2 = dt_calc$next_eps2,
    ltg = dt_calc$next_ltg_decimal, payout_ratio = dt_calc$next_payout_ratio,
    g_ss = dt_calc$G_SS_Next, q = dt_calc$ICC, 
    future.seed = TRUE
  )
  # IMPORTANT: Result uses future earnings, so it's in future shares. Must adjust!
  dt_calc[, P_cNext_qCurr := raw_price_next_curr * Split_Adj] 
  
  # Cross-Term 2: Current CF, Future Rate
  # Note: Current CF implies using G_SS from t (G_SS_Current)
  cat("   Calculating f(c_t, q_t+h)...\n")
  dt_calc[, P_cCurr_qNext := future_mapply(
    FUN = get_implied_price_cdz,
    eps1 = dt_calc$eps1, eps2 = dt_calc$eps2,
    ltg = dt_calc$ltg_decimal, payout_ratio = dt_calc$payout_ratio,
    g_ss = dt_calc$G_SS_Current, q = dt_calc$Next_ICC, 
    future.seed = TRUE
  )]
  # Note: P_cCurr_qNext is in Current shares. No split adjustment needed.
  
  # C. FILTER AND DECOMPOSE
  dt_final <- dt_calc[!is.na(P_cNext_qCurr) & !is.na(P_cCurr_qNext)]
  
  if(nrow(dt_final) == 0) next
  
  # Apply Split Adjustment to Future Price
  dt_final[, Price_Next_Adj := next_price * Split_Adj]
  
  # Return Identity
  dt_final[, sum_news := (Price_Next_Adj - price) / price]
  
  # Eq 4 & 5 (CDZ 2013)
  dt_final[, cf := 0.5 * ( (Price_Next_Adj - P_cCurr_qNext)/price + (P_cNext_qCurr - price)/price ) ]
  dt_final[, dr := 0.5 * ( (P_cCurr_qNext - price)/price + (Price_Next_Adj - P_cNext_qCurr)/price ) ]
  
  # D. IDENTITY-PRESERVING WINSORIZATION
  w_prob <- 0.01 
  dt_final[, c("cf", "dr") := {
    bounds <- quantile(cf, probs = c(w_prob, 1 - w_prob), na.rm = TRUE)
    cf_capped <- pmin(pmax(cf, bounds[1]), bounds[2])
    
    # Economic Floor at -1.0 (Stock can't lose > 100% due to CF alone)
    cf_capped <- pmax(cf_capped, -1.0)
    
    # Residual DR to perfectly preserve identity
    dr_capped <- sum_news - cf_capped
    
    # Floor DR at -1.0, push residual back to CF
    cf_final <- ifelse(dr_capped < -1.0, sum_news - (-1.0), cf_capped)
    dr_final <- ifelse(dr_capped < -1.0, -1.0, dr_capped)
    
    .(cf_final, dr_final)
  }, by = date_t] # Cross-sectional by month t
  
  # Extract essential columns
  res <- dt_final[, .(permno, yyyymm, date = date_t, hor = h, cf, dr, sum_news)]
  
  # E. SAVE
  saveRDS(res, file_out)
}

plan(sequential)

# ==============================================================================
# 5. AGGREGATE ALL HORIZONS INTO ONE FILE (Optional)
# ==============================================================================
cat("\nCombining all horizons...\n")
files <- list.files(out_dir, pattern = "hor_\\d+\\.RDS", full.names = TRUE)

if (length(files) > 0) {
  all_news <- rbindlist(lapply(files, readRDS))
  saveRDS(all_news, "../../data/Analyst/news_decomposition_1_to_180.Rds")
  cat("Success! Saved final aggregated dataset.\n")
}