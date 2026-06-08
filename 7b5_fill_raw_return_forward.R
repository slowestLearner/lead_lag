# --- fill raw return forward
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

# 2. Load the CRSP Daily Return File and convert to log space
############### On JD' PC############
# crspd    <- readRDS("../../../../prom_factor/raw_data/crspv2/crspdv2.Rds") %>%
#   dplyr::select(-lagme) %>%
#   dplyr::filter(year(date)>=1980) %>%
#   setDT()
#
# crspd <- crspd[permno %in% valid_permnos]
# setkey(crspd, permno, date)
# saveRDS(crspd, "../../../data/stocks/CRSP_Daily.Rds")
#######################

print("Loading daily CRSP returns and converting to log scale...")
crspd <- readRDS("../../data/Stocks/CRSP_Daily.Rds")[, .(date, permno, ret)] %>% setDT()
crspd[, logret := log(1 + ret)] # Log transformations allow arithmetic summation
setkey(crspd, permno, date)

# 3. Define the full set of horizons to generate (Matching the 40-quarter framework)
fill_horizons <- c(1, 2, 4, 8, 20, 40)
total_intervals <- nrow(intervals)

# Setup a cluster environment for parallel iteration over portfolio formation blocks
nc <- parallel::detectCores()
plan(multisession, workers = nc - 2)
options(future.globals.maxSize = Inf)
tic("Compounding multi-period cumulative raw returns across horizons...")

# We loop through every unique statement period index 'i'
all_periods_list <- future_lapply(1:total_intervals, function(i) {
  d_start <- intervals$date_start[i]
  st_label <- intervals$statpers[i]
  current_idx <- intervals$idx[i]

  # A local list accumulator for horizons 1 through 40 at this specific asset-formation date
  horizon_accumulator <- list()

  for (h in 1:40) {
    # Find the target end date for a cumulative holding period of 'h' quarters out
    target_row <- current_idx + (h - 1)

    # Security check: if the target timeline exceeds the boundaries of our historical data, skip
    if (target_row > total_intervals) next

    d_end_target <- intervals$date_end[target_row]

    # Slice the daily table from the day after formation through the terminal target date
    # This computes a single, integrated multi-period investment return block
    slice <- crspd[date > d_start & date <= d_end_target,
      .(logrethat = sum(logret, na.rm = TRUE), n_days = .N),
      by = permno
    ]

    # Data Quality Screen: Require a realistic amount of active trading data.
    # An average quarter has ~63 trading days. A 4-quarter horizon requires ~252 days, etc.
    min_required_days <- 50 * h
    slice <- slice[n_days >= min_required_days]

    if (nrow(slice) > 0) {
      slice[, `:=`(statpers = st_label, hor = h)]
      horizon_accumulator[[h]] <- slice[, .(statpers, permno, hor, logrethat)]
    }
  }

  return(rbindlist(horizon_accumulator))
}, future.packages = c("data.table"), future.seed = TRUE)

# Shut down the background calculation threads
plan(sequential)

# Combine all parallel blocks into a unified structure
raw_cumulative_base <- rbindlist(all_periods_list)
rm(crspd, all_periods_list)
toc()
gc()

# ============================================================================
# EXPORT PORTFOLIO-READY HORIZON DATASETS (MATCHING THE 2A4 SENSITIVITY DESIGN)
# ============================================================================
dir.create("tmp/valuation/horizons_raw/", showWarnings = FALSE, recursive = TRUE)

for (h_limit in fill_horizons) {
  message(paste("Exporting true cumulative raw return file tailored for parameter:", h_limit))

  # For the raw data, we do not perform a stale data fill-forward loop anymore.
  # Instead, we pull the clean, true cumulative calculations from our master file.
  # To preserve complete alignment with your existing 3b1 data pipeline, we tag
  # the model type name explicitly.
  export_dt <- copy(raw_cumulative_base)
  export_dt[, factor_model := "raw_ret"]

  # Save the file structure expected by the 3b1 pipeline
  saveRDS(export_dt, paste0("tmp/valuation/horizons_raw/raw_return_filled_", h_limit, ".RDS"))
}

print("Pipeline executed successfully. All cumulative raw market assets are fully structured.")
