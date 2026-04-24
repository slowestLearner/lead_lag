# --- keep track of the survival rate in portfolios. TODO: should merge into 2b1 and 2b1
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

for (stock_base in c("all", "large")) {
  # stock_base <- 'all'
  tic("loading data")
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

  # set of signals with matching returns
  data_0 <- ret_data[signal_data,
    .(
      var, yyyymm,
      permno,
      signal
    ),
    on = .(yyyymm, permno),
    nomatch = 0
  ]
  data_0[, signal := signal - mean(signal), .(var, yyyymm)]

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

  # subset to the signals with matching returns
  signal_data <- signal_data[data_0, .(
    var, yyyymm,
    permno,
    signal,
    idx
  ),
  on = .(var, yyyymm, permno),
  nomatch = 0
  ]

  # This is not really used, but in principle can be used to create a version that takes out stocks < 20% NYSE size cutoff
  # spec_data <- CJ(REMOVE_20PCT_STK = c(F)) %>% mutate(spec_idx = row_number())
  toc()

  # loop over specifications
  tic("Preparing data ...")

  # choose stock universe
  # signal_data <- copy(signal_data)
  # if (this_REMOVE_20PCT_STK) {
  #   signal_data <- merge(signal_data, top_stocks, by = c("yyyymm", "permno"))
  # }

  # save here
  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/just_total_with_fm_controls/numobs/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

  out <- data_0[, .(
    hor = 0,
    sum_n = .N,
    # sum_mv = sum(me, na.rm = TRUE), market value is not necessary since we are weighting by signal
    sum_n_pos = sum(1 * (signal >= 0)),
    sum_n_neg = sum(1 * (signal < 0))
  ), .(var, yyyymm)]
  saveRDS(out, paste0(to_dir, 0, ".RDS"))

  # do the same for the following years
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
        signal
      ),
      on = .(idx = idx_target, permno),
      nomatch = 0
    ] %>% na.omit()
    rm(signal_data)
    gc()
    # 3. Demean signal (by group)
    data[, signal := signal - mean(signal), .(var, yyyymm)]

    # 4. Calculate Statistics
    out <- data[, .(
      hor = this_hor + 1,
      sum_n = .N,
      sum_n_pos = sum(1 * (signal >= 0)),
      sum_n_neg = sum(1 * (signal < 0))
    ), .(var, yyyymm)]

    # 5. Save
    saveRDS(out, outfile)

    return(paste("Processed:", this_hor))
  }

  plan(multisession, workers = nc)

  results <- future_lapply(
    0:119, # 0:179,
    FUN = run_horizon_stats,
    signal_data = signal_data,
    ret_data = ret_data,
    to_dir = to_dir,
    future.packages = c("data.table", "magrittr"),
    future.scheduling = 5 # Batches tasks to keep memory fresh
  )
  plan(sequential)

  # Let's join the loose files, save together, and then delete the loose files
  files <- list.files(to_dir, full.names = TRUE)

  plan(multisession, workers = nc)
  out <- rbindlist(future_lapply(files, readRDS))
  plan(sequential)

  to_file <- paste0(substr(to_dir, 1, nchar(to_dir) - 1), ".RDS")
  saveRDS(out, to_file)
  unlink(to_dir, recursive = TRUE)
}

# # -- todel

# source("~/.runmefirst")

# data_new <- j.readDir("tmp/portfolio_results/all/just_total_with_fm_controls/numobs/")
# data_old <- readRDS("tmp/portfolio_results/all/just_total_with_fm_controls/numobs.RDS")
# data_old <- data_old[hor %in% unique(data_new[, hor])]
# data <- merge(data_old, data_new, by = c("yyyymm", "var", "hor"))
# cor(data[, 4:9])[1:3, 4:6]
# table(data[, sum_n_pos.x - sum_n_pos.y])

# # --- Sanity: check fractions
# source("runmefirst.R")
# data <- readRDS("tmp/portfolio_results/all/just_total_with_fm_controls/numobs_archive.RDS")

# # check fraction of survivial
# data_0 <- data[hor == 0][, hor := NULL]
# names(data_0)[3:5] <- paste0(names(data_0)[3:5], "_0")
# data <- merge(data, data_0, by = c("yyyymm", "var"))
# rm(data_0)

# data[, frac_pos := sum_n_pos / sum_n_pos_0]
# data[, frac_neg := sum_n_neg / sum_n_neg_0]
# this_var <- "combined"

# tmp <- data[var == this_var]
# tmp <- rbind(
#   tmp[, .(type = "pos", frac = mean(sum_n_pos / sum_n_pos_0)), hor],
#   tmp[, .(type = "neg", frac = mean(sum_n_neg / sum_n_neg_0)), hor]
# )

# ggplot(tmp, aes(x = hor, y = frac, fill = type)) +
#   geom_line(aes(color = type), lwd = 2) +
#   geom_hline(yintercept = c(0, 1), lty = 3)
