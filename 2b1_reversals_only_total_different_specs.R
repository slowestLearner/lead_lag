# Compute long-run cumulative returns for the total CSM signals. Include different specifications (different residuals)
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

# -- get raw signals
for (stock_base in c("all", "large")) {
  # stock_base <- "large"
  tic("loading data")
  signal_base <- file.path("tmp/raw_data/signals", stock_base, "total_signal")

  # three versions of signals
  df_1 <- readRDS(paste0(signal_base, "/fm_residualized.RDS")) %>% setDT()
  df_2 <- readRDS(paste0(signal_base, "/fm_residualized_super_set.RDS")) %>% setDT()
  df_3 <- readRDS(paste0(signal_base, "/signals_bh.RDS")) %>% setDT()

  # merge signals together
  merge_keys <- c("yyyymm", "var", "permno")
  signal_data <- Reduce(
    function(x, y) merge(x, y, by = merge_keys, all.x = T),
    list(df_1, df_2, df_3)
  ) %>%
    # signal_data <- df_1 %>%
    data.table::melt(
      id.vars = c("yyyymm", "permno", "var"),
      variable.name = "var_type",
      value.name = "signal",
      na.rm = TRUE
    ) %>%
    mutate(var_type = as.character(var_type)) %>%
    setDT()
  rm(df_1, df_2, df_3)
  gc()



  # get stock returns
  ret_data <- readRDS("../../data/Stocks/Monthly_CRSP.RDS") %>%
    mutate(factor_model = "raw") %>%
    select(yyyymm, permno, factor_model, me_1, ret)

  # change timing to start from 1m after the signals
  tmp <- ret_data[, .(yyyymm)] %>%
    unique() %>%
    mutate(mm = yyyymm - 100 * floor(yyyymm / 100)) %>%
    mutate(yyyymm_prev = if_else(mm == 1, yyyymm - 100 + 11, yyyymm - 1)) %>%
    dplyr::select(-mm)

  ret_data <- merge(ret_data, tmp, by = "yyyymm") %>%
    mutate(yyyymm = yyyymm_prev) %>%
    select(-yyyymm_prev) %>%
    dplyr::rename(ret1 = ret, me = me_1) %>%
    setDT()
  rm(tmp)

  # look at the part that overlaps with the stock uinverse
  target_keys <- unique(ret_data[, .(yyyymm, permno)])
  setkey(signal_data, yyyymm, permno)
  setkey(target_keys, yyyymm, permno)
  signal_data <- signal_data[target_keys, nomatch = 0]

  # get common time grid with index 'idx'
  time_data <- ret_data[, .(yyyymm)] %>%
    unique() %>%
    arrange(yyyymm) %>%
    mutate(idx = frank(yyyymm, ties.method = "dense"))
  ret_data <- merge(ret_data, time_data, by = "yyyymm")
  signal_data <- merge(signal_data, time_data, by = "yyyymm")
  rm(time_data)

  toc()

  # save returns, as well as scaling, here
  to_dir_scaling <- paste0("tmp/portfolio_results/", stock_base, "/just_total_with_fm_controls/scaling/")
  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/just_total_with_fm_controls/returns/")
  dir.create(to_dir_scaling, showWarnings = FALSE, recursive = TRUE)
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

  # function to process one horizon
  # this_hor <- 0
  run_horizon <- function(this_hor, signal_data, ret_data, to_dir_scaling, to_dir) {
    tic(paste0("Processing horizon: ", this_hor))

    # save results here
    file_scaling_path <- paste0(to_dir_scaling, "hor_", this_hor, ".RDS")
    file_out_path <- paste0(to_dir, this_hor, ".RDS")

    if (file.exists(file_scaling_path) && file.exists(file_out_path)) {
      return(paste0("Skipped (Exists): ", this_hor))
    }
    # 1. Create the lagged index
    signal_data[, idx_target := idx + this_hor]

    # 2. Perform the join
    # Using 'i' explicitly for clarity
    data <- ret_data[signal_data,
      .(
        yyyymm = i.yyyymm,
        permno,
        factor_model,
        var,
        var_type,
        signal,
        ret_fut = ret1
      ),
      on = .(idx = idx_target, permno),
      nomatch = 0
    ] %>% na.omit()
    signal_data[, idx_target := NULL]

    rm(signal_data, ret_data)
    gc()

    # 3. Demean signal once more
    data[, signal := signal - mean(signal), .(yyyymm, var, var_type, factor_model)]

    # 4. Save scaling
    scale_data <- data[
      , .(hor = this_hor, sum_of_abs_signal_total = sum(abs(signal))),
      .(yyyymm, var, var_type, factor_model)
    ]
    saveRDS(scale_data, file_scaling_path)

    # 5. Apply scaling
    # We can skip 'hor := NULL' on scale_data since we don't reuse it
    data[scale_data,
      sum_of_abs_signal_total := i.sum_of_abs_signal_total,
      on = .(yyyymm, var, var_type, factor_model)
    ]
    rm(scale_data)
    data[, signal := 2 * signal / sum_of_abs_signal_total]
    data[, sum_of_abs_signal_total := NULL]

    # 6. Process portfolio and save
    data[, port_w_ew := signal]
    out <- data[, .(
      weight_type = "ew",
      hor = this_hor + 1,
      ret_fut = sum(ret_fut * port_w_ew),
      sum_w_pos = sum(port_w_ew * (port_w_ew >= 0)),
      sum_w_neg = sum(port_w_ew * (port_w_ew < 0))
    ), .(yyyymm, var, var_type, factor_model)]
    rm(data)
    gc()
    saveRDS(out, file_out_path)
    rm(out)
    return(paste("Done", this_hor))
    toc()
  }

  gc()
  plan(multisession, workers = 2)
  results <- future_lapply(
    0:119, # 10 yaers seems enough
    # 0:3,
    FUN = run_horizon,
    signal_data = signal_data,
    ret_data = ret_data,
    to_dir_scaling = to_dir_scaling,
    to_dir = to_dir,
    future.packages = c("data.table", "magrittr"),
    future.scheduling = 10
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

  files <- list.files(to_dir_scaling, full.names = TRUE)

  plan(multisession, workers = nc)
  out <- rbindlist(future_lapply(files, readRDS))
  plan(sequential)

  to_file <- paste0(substr(to_dir_scaling, 1, nchar(to_dir_scaling) - 1), ".RDS")
  saveRDS(out, to_file)
  unlink(to_dir_scaling, recursive = TRUE)
}

# # --- CHECK: results similar to before? Consistent if restricting to large stocks?
# source("runmefirst.R")

# # new
# data <- readRDS("tmp/portfolio_results/all/just_total_with_fm_controls/returns.RDS") %>%
#   filter(var == "combined") %>%
#   filter(hor <= 120)

# # old
# tmp <- readRDS("../code_202511/tmp/portfolio_results/just_total_with_fm_controls/remove_20pct_stk_FALSE/returns.RDS") %>% filter(var == "combined")
# data <- data[tmp, on = c("yyyymm", "var", "var_type", "factor_model", "weight_type", "hor")] %>% na.omit()

# plot(data[, cor(ret_fut, i.ret_fut, use = "complete.obs"), hor][order(hor)])

# # --- check if the issue comes from shifting time
# source("runmefirst.R")

# # new
# data <- readRDS("tmp/portfolio_results/all/just_total_with_fm_controls/returns.RDS") %>%
#   filter(var == "combined") %>%
#   filter(var_type == "signal") %>%
#   filter(hor == 3)

# # old
# source("~/.runmefirst")
# tmp <- readRDS("../code_202511/tmp/portfolio_results/just_total_with_fm_controls/remove_20pct_stk_FALSE/returns.RDS") %>%
#   filter(var_type == "signal") %>%
#   na.omit()

# out <- tmp[, .(ret_fut = mean(ret_fut, na.rm = T)), .(var, hor)]

# cor(data[, ret_fut], tmp[, ret_fut], use = "complete.obs")

# data <- merge(data[, .(yyyymm, ret_fut)], tmp[, .(yyyymm, ret_fut_old = ret_fut)], by = "yyyymm")
# data <- data[!is.na(ret_fut_old)]
# n <- nrow(data)

# data[, cor(ret_fut, ret_fut_old)]
# cor(data[1:(n - 1), ret_fut], data[2:n, ret_fut_old])
# ccf(data[, ret_fut], data[, ret_fut_old], lag.max = 4)
