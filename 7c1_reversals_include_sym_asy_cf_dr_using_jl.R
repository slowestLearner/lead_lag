library(this.path)
setwd(this.path::this.dir())
source("../runmefirst.R")

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
  print(paste("Processing stock_base:", stock_base))

  tic("loading data")
  if (.Platform$OS.type == "windows") {
    base_dir <- paste0("D:/Dropbox/Leadlag/data/signal_demean/", stock_base)
  } else {
    base_dir <- paste0("~/Dropbox/SpeculativeIdeas/Leadlag/data/signal_demean/", stock_base)
  }

  file_map <- c(
    "Analyst.Rds"     = "analyst",
    "BEAcustomer.Rds" = "beacustomer",
    "BEAsupplier.Rds" = "beasupplier",
    "Econ.Rds"        = "econ",
    "Geo.Rds"         = "geo",
    "Indu.Rds"        = "industry",
    "Pseudo.Rds"      = "pseudo",
    "Tec.Rds"         = "tech"
  )

  # Load and bind signals
  data_list <- lapply(names(file_map), function(f_name) {
    full_path <- file.path(base_dir, f_name)
    if (!file.exists(full_path)) {
      return(NULL)
    }
    dt <- readRDS(full_path)
    dt[, var := file_map[[f_name]]]
    return(dt)
  })

  # compute total signal
  signal_data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  rm(data_list)
  setnames(signal_data, "signal_s", "signal_sym")
  setnames(signal_data, "signal_a", "signal_asy")
  signal_data[, signal_total := signal_sym + signal_asy]

  # combined signal calculation
  vars_per_month <- signal_data[, .(n_total_vars = uniqueN(var)), by = yyyymm]
  signal_data[vars_per_month, on = "yyyymm", N_denom := i.n_total_vars]
  data_combined <- signal_data[, .(
    var = "combined",
    signal_sym = sum(signal_sym, na.rm = TRUE) / N_denom[1],
    signal_asy = sum(signal_asy, na.rm = TRUE) / N_denom[1],
    signal_total = sum(signal_total, na.rm = TRUE) / N_denom[1]
  ), by = .(yyyymm, permno)]
  signal_data[, N_denom := NULL]
  signal_data <- rbind(signal_data, data_combined)
  rm(data_combined)

  # melt into long format
  signal_data <- data.table::melt(signal_data,
    id.vars = c("yyyymm", "permno", "var"),
    variable.name = "var_type", value.name = "signal"
  )
  signal_data[, var_type := gsub("signal_", "", as.character(var_type))]
  setkey(signal_data, yyyymm, permno)

  # ============================================================================
  # SENSITIVITY LOOP FOR FILL-FORWARD HORIZONS
  # ============================================================================
  fill_horizons <- c(1, 2, 4, 8, 20, 40)
  for (h_limit in fill_horizons) {
    # h_limit <- max(fill_horizons)
    input_file <- paste0("tmp/valuation/horizons/implied_forward_cf_return_filled_", h_limit, ".RDS")
    if (!file.exists(input_file)) next

    # Load and clean return data
    ret_raw <- readRDS(input_file) %>%
      dplyr::transmute(permno,
        yyyymm = floor(statpers / 100), hor_val = hor,
        ret_fut = logrethat, factor_model = "cf"
      ) %>%
      setDT()

    # Define target horizons: t-60 to t-3 (past) and t+3 to t+120 (future)
    target_horizons <- c(seq(-60, -3, by = 3), seq(3, 120, by = 3))

    to_dir_scaling <- paste0("tmp/portfolio_results/", stock_base, "/h", h_limit, "/scaling/")
    to_dir <- paste0("tmp/portfolio_results/", stock_base, "/h", h_limit, "/returns/")
    dir.create(to_dir_scaling, showWarnings = FALSE, recursive = TRUE)
    dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

    # --- CRITICAL MEMORY FIX: PRE-SPLIT DATA ---
    # Instead of passing ret_raw (7GB+) to every worker, split it by hor_val
    ret_list_split <- split(ret_raw, by = "hor_val")
    rm(ret_raw)
    gc()

    # Define the worker function
    run_horizon <- function(h, signal_data, ret_list_split, to_dir_scaling, to_dir) {
      file_scaling_path <- paste0(to_dir_scaling, "hor_", h, ".RDS")
      file_out_path <- paste0(to_dir, h, ".RDS")

      if (file.exists(file_scaling_path) && file.exists(file_out_path)) {
        return(NULL)
      }

      # Select only the specific return slice needed for this horizon
      this_ret <- copy(ret_list_split[[as.character(abs(h) / 3)]])
      if (is.null(this_ret)) {
        return(NULL)
      }

      # Pre-formation logic
      if (h < 0) {
        this_ret[, yyyymm := as.integer(format(as.Date(paste0(yyyymm, "01"), "%Y%m%d") %m+% months(abs(h)), "%Y%m"))]
      }

      data <- signal_data[this_ret, on = .(yyyymm, permno), nomatch = 0]
      data <- data[!is.na(ret_fut) & !is.na(signal)]
      if (nrow(data) == 0) {
        return(NULL)
      }

      data[, signal := signal - mean(signal), by = .(yyyymm, var, var_type, factor_model)]

      scale_data <- data[var_type == "total", .(hor = h, sum_of_abs_signal_total = sum(abs(signal))),
        by = .(yyyymm, var, factor_model)
      ]
      saveRDS(scale_data, file_scaling_path)

      data[scale_data, sum_of_abs_signal_total := i.sum_of_abs_signal_total, on = .(yyyymm, var, factor_model)]
      data[, signal := 2 * signal / sum_of_abs_signal_total]

      out <- data[, .(
        weight_type = "ew", hor = h, ret_fut = sum(ret_fut * signal),
        sum_w_pos = sum(signal[signal > 0]), sum_w_neg = sum(signal[signal < 0])
      ), by = .(yyyymm, var, var_type, factor_model)]

      saveRDS(out, file_out_path)
      return(NULL)
    }

    # Parallel setup
    nc <- parallel::detectCores()
    # Increase to 16GB limit for safety on large servers
    options(future.globals.maxSize = +Inf)
    plan(multisession, workers = nc - 2)

    results <- future_lapply(
      target_horizons,
      FUN = run_horizon,
      signal_data = signal_data,
      ret_list_split = ret_list_split, # Much smaller footprint per worker
      to_dir_scaling = to_dir_scaling,
      to_dir = to_dir,
      future.packages = c("data.table", "lubridate"),
      future.seed = TRUE
    )
    plan(sequential)

    # Combine results
    files_ret <- list.files(to_dir, full.names = TRUE)
    if (length(files_ret) > 0) {
      out <- rbindlist(lapply(files_ret, readRDS))
      saveRDS(out, paste0("tmp/portfolio_results/", stock_base, "/h", h_limit, "/returns.RDS"))
      unlink(to_dir, recursive = TRUE)
    }

    files_sc <- list.files(to_dir_scaling, full.names = TRUE)
    if (length(files_sc) > 0) {
      out_sc <- rbindlist(lapply(files_sc, readRDS))
      saveRDS(out_sc, paste0("tmp/portfolio_results/", stock_base, "/h", h_limit, "/scaling.RDS"))
      unlink(to_dir_scaling, recursive = TRUE)
    }

    rm(ret_list_split)
    gc()
  }
}

# TODO I think JD saved his results into something called "cf_dr". That also did not have pre-periods
