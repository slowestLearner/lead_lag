# Compute cumulative portfolio returns for both total and and sym/asy components
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

for (stock_base in c("all", "large")) {
  # stock_base <- 'all'

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


  data_list <- lapply(names(file_map), function(f_name) {
    full_path <- file.path(base_dir, f_name)

    if (!file.exists(full_path)) {
      warning(paste("File not found:", full_path))
      return(NULL)
    }

    # Read and add the 'var' column immediately
    dt <- readRDS(full_path)
    dt[, var := file_map[[f_name]]]
    return(dt)
  })

  # Combine all at once (Ultra fast)
  data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  rm(data_list)
  setnames(data, "signal_s", "signal_sym")
  setnames(data, "signal_a", "signal_asy")
  data[, signal_total := signal_sym + signal_asy]

  # combined signal = average over the rest
  vars_per_month <- data[, .(n_total_vars = uniqueN(var)), by = yyyymm]
  data[vars_per_month, on = "yyyymm", N_denom := i.n_total_vars]
  data_combined <- data[, .(
    var = "combined",
    signal_sym = sum(signal_sym, na.rm = TRUE) / N_denom[1],
    signal_asy = sum(signal_asy, na.rm = TRUE) / N_denom[1],
    signal_total = sum(signal_total, na.rm = TRUE) / N_denom[1]
  ), by = .(yyyymm, permno)]
  data[, N_denom := NULL]
  data <- rbind(data, data_combined)
  rm(data_combined)


  # melt into long format
  data <- data.table::melt(data, id.vars = c("yyyymm", "permno", "var"), variable.name = "var_type", value.name = "signal") %>%
    mutate(var_type = as.character(var_type)) %>%
    setDT()
  data[, var_type := gsub("signal_", "", var_type)]
  signal_data <- copy(data)
  rm(data)

  # -- get stock returns
  ret_data <- readRDS("../../data/Stocks/Monthly_CRSP.RDS") %>%
    mutate(factor_model = "raw") %>%
    select(yyyymm, permno, factor_model, me_1, ret) %>%
    setDT()

  # change timing to start from 1m after the signals
  tmp <- ret_data[, .(yyyymm)] %>%
    unique() %>%
    mutate(mm = yyyymm - 100 * floor(yyyymm / 100)) %>%
    mutate(yyyymm_prev = if_else(mm == 1, yyyymm - 100 + 11, yyyymm - 1)) %>%
    select(-mm)

  ret_data <- merge(ret_data, tmp, by = "yyyymm") %>%
    mutate(yyyymm = yyyymm_prev) %>%
    select(-yyyymm_prev) %>%
    dplyr::rename(ret1 = ret, me = me_1)
  rm(tmp)

  # need to have stocks
  setDT(signal_data)
  setDT(ret_data)
  target_keys <- unique(ret_data[, .(yyyymm, permno)])
  setkey(signal_data, yyyymm, permno)
  setkey(target_keys, yyyymm, permno)
  signal_data <- signal_data[target_keys, nomatch = 0]

  # # ont used yet: for the "subsetting to top 80%", can just have the subsetting "keep" file here
  # top_stocks <- unique(ret_data[keep == 1, .(yyyymm, permno)])

  # get common time grid with index 'idx'
  time_data <- ret_data[, .(yyyymm)] %>%
    unique() %>%
    arrange(yyyymm) %>%
    mutate(idx = frank(yyyymm, ties.method = "dense")) %>%
    setDT()
  ret_data <- merge(ret_data, time_data, by = "yyyymm")
  signal_data <- merge(signal_data, time_data, by = "yyyymm")
  rm(time_data)

  # different specifications: not used yet
  # spec_data <- CJ(REMOVE_20PCT_STK = c(F)) %>% mutate(spec_idx = row_number())
  toc()
  gc()
  # loop over specifications

  # # parse spec
  # this_REMOVE_20PCT_STK <- spec_data[this_spec, REMOVE_20PCT_STK]
  # tic(paste0("Preparing data for spec = ", this_spec))
  tic("Start main data processing")

  # choose stock universe
  # signal_data <- copy(signal_data)
  # if (this_REMOVE_20PCT_STK) {
  #   signal_data <- merge(signal_data, top_stocks, by = c("yyyymm", "permno"))
  # }
  # rm(signal_data)

  # Let's get future portfolio returns and save
  to_dir_scaling <- to_dir <- paste0("tmp/portfolio_results/", stock_base, "/scale_by_total/scaling/")
  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/scale_by_total/returns/")
  dir.create(to_dir_scaling, showWarnings = FALSE, recursive = TRUE)
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

  run_horizon <- function(this_hor, signal_data, ret_data, to_dir_scaling, to_dir) {
    tic(paste0("Processing horizon: ", this_hor))


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
    ]
    rm(signal_data, ret_data)

    data <- data[!is.na(ret_fut) & !is.na(signal)]
    # 3. Demean signal
    data[, signal := signal - mean(signal), .(yyyymm, var, var_type, factor_model)]

    # 4. Save scaling
    scale_data <- data[
      var_type == "total", .(hor = this_hor, sum_of_abs_signal_total = sum(abs(signal))),
      .(yyyymm, var, factor_model)
    ] # very important to scale by the total!!!!

    saveRDS(scale_data, file_scaling_path)

    # 5. Apply scaling
    # We can skip 'hor := NULL' on scale_data since we don't reuse it
    data[scale_data,
      sum_of_abs_signal_total := i.sum_of_abs_signal_total,
      on = .(yyyymm, var, factor_model)
    ]
    rm(scale_data)
    data[, signal := 2 * signal / sum_of_abs_signal_total]
    data[, sum_of_abs_signal_total := NULL]



    out <- data[, .(
      weight_type = "ew",
      hor = this_hor + 1,
      ret_fut = sum(ret_fut * signal),
      sum_w_pos = sum(signal[signal > 0]), # Subsetting vector is faster than logical multiplication
      sum_w_neg = sum(signal[signal < 0])
    ), .(yyyymm, var, var_type, factor_model)]
    rm(data)
    gc()
    saveRDS(out, file_out_path)
    rm(out)
    return(paste("Done", this_hor))
    toc()
  }

  plan(multisession, workers = 2)
  results <- future_lapply(
    # 0:119,
    120:179,
    FUN = run_horizon,
    signal_data = signal_data,
    ret_data = ret_data,
    to_dir_scaling = to_dir_scaling,
    to_dir = to_dir,
    future.packages = c("data.table", "magrittr"),
    future.scheduling = 5
  )
  plan(sequential)

  # Let's join them together
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



# # --- sanity: plot results
# source("runmefirst.R")

# # get data
# out <- readRDS("tmp/portfolio_results/all/scale_by_total/returns.RDS") %>%
#   select(-factor_model, -weight_type, -sum_w_pos, sum_w_neg) %>%
#   mutate(source = "all")
# tt <- readRDS("tmp/portfolio_results/large/scale_by_total/returns.RDS") %>%
#   select(-factor_model, -weight_type, -sum_w_pos, sum_w_neg) %>%
#   mutate(source = "large")
# out <- rbind(out, tt)
# rm(tt)

# # get cumulative returns
# by_vars <- c("yyyymm", "var", "var_type", "source")
# setorderv(out, c(by_vars, "hor"))
# out[, cumret_fut := cumprod(1 + ret_fut) - 1, by = by_vars]

# # summarize averages
# by_vars <- c("var", "var_type", "source", "hor")
# out_summary <- out[, .(cumret_fut = mean(cumret_fut)), by = by_vars]

# out <- copy(out_summary[var == "combined" & var_type == "total"])
# ggplot(out, aes(x = hor, y = cumret_fut, color = source)) +
#   geom_line(lwd = 4) +
#   theme_minimal() +
#   theme(text = element_text(size = 30))
