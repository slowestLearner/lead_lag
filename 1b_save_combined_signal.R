# --- We often use the combined signal. This script computes it once and save it.
# TODO: put into the raw "demean" folder later
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
  tic("Processing stock_base: ", stock_base)
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

  # Combine and focus on the total signal
  data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  setnames(data, "signal_s", "signal_sym")
  setnames(data, "signal_a", "signal_asy")
  data[, signal := signal_sym + signal_asy]

  # combined signal = average over the rest
  vars_per_month <- data[, .(n_total_vars = uniqueN(var)), by = yyyymm]
  data[vars_per_month, on = "yyyymm", N_denom := i.n_total_vars]
  data_combined <- data[, .(
    signal = sum(signal, na.rm = TRUE) / N_denom[1],
    signal_sym = sum(signal_sym, na.rm = TRUE) / N_denom[1],
    signal_asy = sum(signal_asy, na.rm = TRUE) / N_denom[1]
  ), by = .(yyyymm, permno)]

  to_file <- paste0("tmp/processed_signals/", stock_base, "/combined_signal.RDS")
  dir.create(dirname(to_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(data_combined, to_file)
  toc()
}
