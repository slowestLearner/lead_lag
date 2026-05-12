library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")
library(tidyr)
library(data.table)
library(dplyr)
library(future.apply)

for (stock_base in c("all", "large")) {
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
    if (!file.exists(full_path)) return(NULL)
    dt <- readRDS(full_path)
    dt[, var := file_map[[f_name]]]
    return(dt)
  })
  
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
  data <- data.table::melt(data, id.vars = c("yyyymm", "permno", "var"), 
                           variable.name = "var_type", value.name = "signal") %>%
    mutate(var_type = as.character(var_type)) %>%
    setDT()
  data[, var_type := gsub("signal_", "", var_type)]
  signal_data <- copy(data)
  rm(data)
  
  # ============================================================================
  # LOAD NEW MULTI-HORIZON DECOMPOSITION DATA
  # ============================================================================
  # The new data has: permno, yyyymm (base month t), hor (1 to 180), cf, dr, sum_news
  
  ret_data <- readRDS("../../data/Analyst/news_decomposition_1_to_180.Rds") %>%
    dplyr::select(yyyymm, permno, hor, cf, dr, sum_news) %>%
    # Pivot to long format for factor models
    pivot_longer(cols = c("cf", "dr", "sum_news"), 
                 names_to = "factor_model", 
                 values_to = "ret_fut") %>%
    setDT()
  
  # IMPORTANT: We DO NOT need to shift the dates backwards anymore!
  # The 'yyyymm' in the new data is already the base month t. 
  # A row with yyyymm=198501 and hor=1 represents the change from 198501 to 198502.
  # This aligns perfectly with the signal generated in 198501.
  
  # Filter signal_data to only include valid firm-months
  target_keys <- unique(ret_data[, .(yyyymm, permno)])
  setkey(signal_data, yyyymm, permno)
  setkey(target_keys, yyyymm, permno)
  signal_data <- signal_data[target_keys, nomatch = 0]
  
  toc()
  gc()
  
  # ============================================================================
  # PREPARE FOR PARALLEL EXECUTION
  # ============================================================================
  tic("Start main data processing")
  
  to_dir_scaling <- paste0("tmp/portfolio_results/", stock_base, "/cf_dr/scaling/")
  to_dir         <- paste0("tmp/portfolio_results/", stock_base, "/cf_dr/returns/")
  dir.create(to_dir_scaling, showWarnings = FALSE, recursive = TRUE)
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  
  # MEMORY OPTIMIZATION: Split the massive return dataset by horizon
  # This prevents RAM blowouts during parallel execution
  ret_list <- split(ret_data, by = "hor")
  rm(ret_data)
  gc()
  
  # Update function to take a sliced dataset rather than the whole thing
  run_horizon <- function(this_ret_data, signal_data, to_dir_scaling, to_dir) {
    this_hor <- this_ret_data$hor[1]
    tic(paste0("Processing horizon: ", this_hor))
    
    file_scaling_path <- paste0(to_dir_scaling, "hor_", this_hor, ".RDS")
    file_out_path     <- paste0(to_dir, this_hor, ".RDS")
    
    if (file.exists(file_scaling_path) && file.exists(file_out_path)) {
      return(paste0("Skipped (Exists): ", this_hor))
    }
    
    # 1. Join directly on yyyymm and permno
    data <- signal_data[this_ret_data, 
                        on = .(yyyymm, permno), 
                        nomatch = 0, 
                        allow.cartesian = TRUE]
    
    data <- data[!is.na(ret_fut) & !is.na(signal)]
    if(nrow(data) == 0) return(paste("No data for", this_hor))
    
    # 2. Demean signal
    data[, signal := signal - mean(signal), by = .(yyyymm, var, var_type, factor_model)]
    
    # 3. Save scaling
    scale_data <- data[
      var_type == "total", .(hor = this_hor, sum_of_abs_signal_total = sum(abs(signal))),
      by = .(yyyymm, var, factor_model)
    ]
    saveRDS(scale_data, file_scaling_path)
    
    # 4. Apply scaling
    data[scale_data, sum_of_abs_signal_total := i.sum_of_abs_signal_total, on = .(yyyymm, var, factor_model)]
    data[, signal := 2 * signal / sum_of_abs_signal_total]
    
    # 5. Calculate Portfolio Returns
    out <- data[, .(
      weight_type = "ew",
      hor = this_hor, # Horizon maps 1:1 with the data's 'hor' column
      ret_fut = sum(ret_fut * signal),
      sum_w_pos = sum(signal[signal > 0]), 
      sum_w_neg = sum(signal[signal < 0])
    ), by = .(yyyymm, var, var_type, factor_model)]
    
    saveRDS(out, file_out_path)
    
    toc()
    return(paste("Done", this_hor))
  }
  
  # ============================================================================
  # EXECUTE PARALLEL LOOP
  # ============================================================================
  nc <- parallel::detectCores()
  # Set memory limit higher for large merges
  options(future.globals.maxSize = 4000 * 1024^2) 
  plan(multisession, workers = nc - 2)
  
  results <- future_lapply(
    ret_list,
    FUN = run_horizon,
    signal_data = signal_data,
    to_dir_scaling = to_dir_scaling,
    to_dir = to_dir,
    future.packages = c("data.table", "magrittr"),
    future.seed = TRUE
  )
  plan(sequential)
  
  # ============================================================================
  # COMBINE AND CLEAN UP
  # ============================================================================
  # Join Portfolio Returns
  files <- list.files(to_dir, full.names = TRUE)
  out <- rbindlist(lapply(files, readRDS))
  to_file <- paste0(substr(to_dir, 1, nchar(to_dir) - 1), ".RDS")
  saveRDS(out, to_file)
  unlink(to_dir, recursive = TRUE)
  
  # Join Scaling Data
  files_sc <- list.files(to_dir_scaling, full.names = TRUE)
  out_sc <- rbindlist(lapply(files_sc, readRDS))
  to_file_sc <- paste0(substr(to_dir_scaling, 1, nchar(to_dir_scaling) - 1), ".RDS")
  saveRDS(out_sc, to_file_sc)
  unlink(to_dir_scaling, recursive = TRUE)
  
  rm(ret_list)
  gc()
}