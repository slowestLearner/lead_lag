# --- keep track of the survival rate in portfolios. TODO: should merge into 2b1 and 2b1
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")
options(width = 200)

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
  print(paste0("loading data, stock_base: ", stock_base))
  tic()
  signal_base <- file.path("tmp/raw_data/signals", stock_base, "total_signal")

  # read fm_residualized
  signal_data <- readRDS(paste0(signal_base, "/fm_residualized.RDS")) %>%
    data.table() %>%
    data.table::melt(id.vars = c("yyyymm", "permno", "var"), variable.name = "var_type", value.name = "signal") %>%
    mutate(var_type = as.character(var_type)) %>%
    dplyr::filter(var_type == "signal") %>%
    dplyr::select(-var_type) %>%
    setDT()

  # get stock returns
  ret_data <- readRDS("../../data/Stocks/Monthly_CRSP.RDS") %>%
    transmute(yyyymm, permno, me_1, keep, ret) %>%
    setDT()

  signal_data[, signal := signal - mean(signal), .(var, yyyymm)]

  # restrict to stocks with data
  signal_data <- merge(signal_data, ret_data[, .(yyyymm, permno)], by = c("yyyymm", "permno"))

  # more granular bins
  signal_data[, dir := sign(signal)]
  signal_data[, bin := ntile(abs(signal), 10), .(yyyymm, var, dir)]
  signal_data[, signal := NULL]

  # change timing to start from 1m after the signals
  tmp <- ret_data[, .(yyyymm)] %>%
    unique() %>%
    mutate(mm = yyyymm - 100 * floor(yyyymm / 100)) %>%
    mutate(yyyymm_prev = if_else(mm == 1, yyyymm - 100 + 11, yyyymm - 1)) %>%
    dplyr::select(-mm)

  ret_data <- merge(ret_data, tmp, by = "yyyymm") %>%
    mutate(yyyymm = yyyymm_prev) %>%
    select(-yyyymm_prev) %>%
    dplyr::rename(ret1 = ret, me = me_1)
  rm(tmp)

  # look at the part that overlaps with the stock uinverse
  setDT(signal_data)
  setDT(ret_data)
  target_keys <- unique(ret_data[, .(yyyymm, permno)])
  setkey(signal_data, yyyymm, permno)
  setkey(target_keys, yyyymm, permno)

  # get common time grid with index 'idx'
  time_data <- ret_data[, .(yyyymm)] %>%
    unique() %>%
    arrange(yyyymm) %>%
    mutate(idx = frank(yyyymm, ties.method = "dense"))
  ret_data <- merge(ret_data, time_data, by = "yyyymm")
  signal_data <- merge(signal_data, time_data, by = "yyyymm")
  rm(time_data)
  toc()


  # loop over specifications
  print(paste0("Processing survival rate for stock_base: ", stock_base))
  tic()

  # save here
  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/just_total_with_fm_controls/numobs/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

  # initial period
  out <- signal_data[, .(
    hor = 0,
    sum_n = .N,
    # sum_mv = sum(me, na.rm = TRUE), market value is not necessary since we are weighting by signal
    sum_n_pos = sum(dir == 1),
    sum_n_neg = sum(dir == -1),
    sum_n_pos_bin10 = sum(dir == 1 & bin %in% c(10)),
    sum_n_neg_bin10 = sum(dir == -1 & bin %in% c(10)),
    sum_n_pos_bin9_to_10 = sum(dir == 1 & bin %in% c(9:10)),
    sum_n_neg_bin9_to_10 = sum(dir == -1 & bin %in% c(9:10)),
    sum_n_pos_bin6_to_10 = sum(dir == 1 & bin %in% c(6:10)),
    sum_n_neg_bin6_to_10 = sum(dir == -1 & bin %in% c(6:10))
  ), .(var, yyyymm)]
  saveRDS(out, paste0(to_dir, 0, ".RDS"))

  # do the same for the following years
  # this_hor <- 1
  run_horizon_stats <- function(this_hor, signal_data, ret_data, to_dir) {
    # --- Sanity Check: Skip if file exists ---
    # Note: You used (this_hor + 1) in your file naming convention
    outfile <- paste0(to_dir, this_hor + 1, ".RDS")

    if (file.exists(outfile)) {
      return(NULL) # Skip silently
    }

    # --- Logic ---
    # 1. Shift index
    signal_data[, idx_target := idx + this_hor]

    # 2. Join (Inner join via nomatch=0)
    data <- ret_data[signal_data,
      .(var,
        yyyymm = i.yyyymm,
        permno,
        dir,
        # signal,
        bin = i.bin
      ),
      on = .(idx = idx_target, permno),
      nomatch = 0
    ] %>% na.omit()
    gc()

    # 3. Calculate Statistics
    out <- data[, .(
      hor = this_hor + 1,
      sum_n = .N,
      sum_n_pos = sum(dir == 1),
      sum_n_neg = sum(dir == -1),
      sum_n_pos_bin10 = sum(dir == 1 & bin %in% c(10)),
      sum_n_neg_bin10 = sum(dir == -1 & bin %in% c(10)),
      sum_n_pos_bin9_to_10 = sum(dir == 1 & bin %in% c(9:10)),
      sum_n_neg_bin9_to_10 = sum(dir == -1 & bin %in% c(9:10)),
      sum_n_pos_bin6_to_10 = sum(dir == 1 & bin %in% c(6:10)),
      sum_n_neg_bin6_to_10 = sum(dir == -1 & bin %in% c(6:10))
    ), .(var, yyyymm)]


    # 5. Save
    saveRDS(out, outfile)
    return(paste("Processed:", this_hor))
  }

  # plan(multisession, workers = nc)
  plan(multicore, workers = nc)

  results <- future_lapply(
    0:119, # 0:179,
    FUN = run_horizon_stats,
    signal_data = signal_data,
    ret_data = ret_data,
    to_dir = to_dir,
    future.packages = c("data.table", "magrittr"),
    future.scheduling = 5, # Batches tasks to keep memory fresh
    future.seed = 123
  )
  plan(sequential)

  # Let's join the loose files, save together, and then delete the loose files
  files <- list.files(to_dir, full.names = TRUE)
  out <- rbindlist(lapply(files, readRDS))
  to_file <- paste0(substr(to_dir, 1, nchar(to_dir) - 1), ".RDS")
  dir.create(dirname(to_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(out, to_file)
  unlink(to_dir, recursive = TRUE)
  toc()
}
