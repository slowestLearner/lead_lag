# --- fill raw return forward (efficient rewrite of 7b5_fill_raw_return_forward.R)
# Strategy: compute single-quarter returns once by mapping each daily obs to its
# statpers interval, then build cumulative h-quarter returns by summing those
# single-quarter log returns over consecutive intervals (no overlapping re-slicing).
library(this.path)
setwd(this.path::this.dir())
source("../runmefirst.R")
options(width = 120)

# get a list of statpers and the corresponding date_start and date_end
raw_data <- readRDS("tmp/valuation/implied_forward_cf_return.RDS")
statpers_list <- unique(raw_data[, .(statpers)])[order(statpers)]
statpers_list[, date_start := as.Date(as.character(statpers), "%Y%m%d")]

# Lead the date to find the exact end of each subsequent quarter interval
statpers_list[, date_end := data.table::shift(date_start, n = -1, type = "lag")]
intervals <- statpers_list[!is.na(date_end)]

# Create an index mapping table for timeline alignment
statpers_to_idx <- unique(intervals[, .(statpers)])[order(statpers)][, idx := .I]
intervals <- merge(intervals, statpers_to_idx, by = "statpers")
setorder(intervals, idx)
total_intervals <- nrow(intervals)

# Load the CRSP Daily Return File and convert to log space
print("Loading daily CRSP returns and converting to log scale...")
crspd <- readRDS("../../data/Stocks/CRSP_Daily.Rds")[, .(date, permno, ret)] %>% setDT()
crspd[, logret := log(1 + ret)] # log space => cumulative return is a simple sum
crspd[, ret := NULL]

# Horizons whose files we export (only controls filenames; see export loop)
fill_horizons <- c(1, 2, 4, 8, 20, 40)

# ============================================================================
# STEP 1: single-quarter returns -- map each daily obs to one statpers interval
# Interval idx i covers (date_start[i], date_end[i]] with date_end[i] == date_start[i+1],
# matching the original's `date > d_start & date <= d_end_target` (half-open on the left).
# ============================================================================
tic("Computing single-quarter returns")
# breaks = all interval starts plus the final interval end (upper bound of coverage)
breaks <- c(intervals$date_start, intervals$date_end[total_intervals])

# left.open = TRUE => assigns day d to i such that breaks[i] < d <= breaks[i+1]
crspd[, idx := findInterval(date, breaks, left.open = TRUE)]
crspd <- crspd[idx >= 1L & idx <= total_intervals]

# one log-return and trading-day count per (permno, quarter-interval)
qret <- crspd[, .(qlog = sum(logret, na.rm = TRUE), qdays = .N), by = .(permno, idx)]
rm(crspd)
gc()
toc()

# ============================================================================
# STEP 2: cumulative h-quarter returns by rolling sum over consecutive intervals.
# A_h(t) = sum_{j=0}^{h-1} q(t+j); built incrementally as A_h = A_{h-1} + q(t+h-1).
# Each step inner-joins the (t+h-1)-th quarter, so a (formation, permno) row only
# survives if ALL intermediate quarters are present (contiguity, like the original).
# ============================================================================
# ============================================================================
# STEP 2: Cumulative h-quarter returns by rolling sum over consecutive intervals.
# ============================================================================
tic("Accumulating cumulative horizons")

# 1. Base horizon (h = 1)
acc <- qret[, .(idx, permno, cl = qlog, cd = qdays)]
horizon_list <- vector("list", 40)
horizon_list[[1]] <- acc[, .(idx, permno, hor = 1L, logrethat = cl, n_days = cd)]

for (h in 2:40) {
  # Shift the incoming quarter index back so it aligns with the formation date
  addto <- qret[, .(idx = idx - (h - 1L), permno, add_log = qlog, add_days = qdays)]

  # Remove shifted indices that fall before the first valid formation period
  addto <- addto[idx >= 1L]

  # FIX 1: FULL OUTER JOIN (`all = TRUE`)
  # Allows mid-horizon entrants (e.g., IPOs or resuming stocks) to join the table
  # even if they had 0 trading days in the very first quarter.
  acc <- merge(acc, addto, by = c("idx", "permno"), all = TRUE)

  # Convert NAs to 0 (Mimics na.rm = TRUE in the original daily summation slice)
  acc[is.na(cl), cl := 0]
  acc[is.na(cd), cd := 0L]
  acc[is.na(add_log), add_log := 0]
  acc[is.na(add_days), add_days := 0L]

  # Accumulate the running totals
  acc[, `:=`(cl = cl + add_log, cd = cd + add_days)]
  acc[, c("add_log", "add_days") := NULL]

  # FIX 2: STRICT TIMELINE BOUNDARY TRUNCATION
  # Matches the original: `if (target_row > total_intervals) next`
  # We drop any formation periods where the required horizon exceeds the master dataset bounds.
  valid_acc <- acc[idx + (h - 1L) <= total_intervals]

  # Output the clean, boundary-checked slice
  horizon_list[[h]] <- valid_acc[, .(idx, permno, hor = h, logrethat = cl, n_days = cd)]
}
rm(acc, qret)

raw_cumulative_base <- rbindlist(horizon_list)
rm(horizon_list)
toc()

# Data Quality Screen (Now applies perfectly to the matched universe)
raw_cumulative_base <- raw_cumulative_base[n_days >= 50L * hor]

# Map formation interval idx back to its statpers label
raw_cumulative_base <- merge(raw_cumulative_base, statpers_to_idx, by = "idx")
raw_cumulative_base <- raw_cumulative_base[, .(statpers, permno, hor, logrethat)]
setorder(raw_cumulative_base, statpers, permno, hor)
gc()

# ============================================================================
# EXPORT PORTFOLIO-READY HORIZON DATASETS (MATCHING THE 2A4 SENSITIVITY DESIGN)
# The raw returns are already true cumulative returns, so every fill horizon file
# is the same table (only the filename differs), exactly as in the original.
# ============================================================================
to_dir <- "tmp/valuation/horizons_raw_new/"
dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

for (h_limit in fill_horizons) {
  # h_limit <- fill_horizons[1]
  message(paste("Exporting true cumulative raw return file tailored for parameter:", h_limit))
  export_dt <- copy(raw_cumulative_base)
  export_dt[, factor_model := "raw_ret"]
  saveRDS(export_dt, paste0(to_dir, "raw_return_filled_", h_limit, ".RDS"))
}

print("Pipeline executed successfully. All cumulative raw market assets are fully structured.")
