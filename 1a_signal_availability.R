# --- Computes and saves monthly equal- and value-weighted data availability statistics for
# various cross-firm lead-lag signals relative to the CRSP stock universe.
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

# all stocks that satisfy the filter
data <- readRDS("../../data/Stocks/Monthly_CRSP.RDS")
data <- data[, list(yyyymm, permno, me_1)]


for (stock_base in c("all", "large")) {
  # stock_base <- 'all'
  tic("loading data")
  if (.Platform$OS.type == "windows") {
    base_dir <- paste0("D:/Dropbox/Leadlag/data/signal_demean/", stock_base)
  } else {
    base_dir <- paste0("~/Dropbox/SpeculativeIdeas/Leadlag/data/signal_demean/", stock_base)
  }

  # map from file name to signal name
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

  # read all signals
  data_list <- lapply(names(file_map), function(f_name) {
    # f_name <- names(file_map)[1]
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
  tmp <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  col_keep <- c("permno", "yyyymm", "var", "signal_s")
  tmp <- tmp[, ..col_keep]
  tmp <- tmp[!is.na(signal_s)] # rquire having signals

  # for each (year-month, signal), require having 100 observations to use
  tt <- tmp[, list(obs = length(permno)), list(yyyymm, var)]
  tt <- tt[obs >= 100][, obs := NULL]
  tmp <- merge(tmp, tt, by = c("yyyymm", "var"))
  rm(tt)
  tmp[, has_data := 1]
  # tmp[, value := NULL]
  tmp <- merge(tmp, unique(data[, list(yyyymm, permno)]), by = c("yyyymm", "permno")) # require having stock data

  # summarize fraction of stocks covered for each CSM predictor
  p.get_one <- function(this_v) {
    # this_v <- 'industry'
    out <- copy(data)
    out <- merge(out, tmp[var == this_v, list(yyyymm, permno, has_data)], by = c("yyyymm", "permno"), all.x = TRUE) # NOTE: changed
    out[is.na(has_data), has_data := 0]
    out <- out[, list(
      var = this_v, obs_all = sum(has_data),
      frac_with_data_ew = mean(has_data),
      frac_with_data_vw = weighted.mean(has_data, me_1)
    ), list(yyyymm)]
    return(out)
  }

  vv <- unique(tmp[, var])
  out <- rbindlist(lapply(vv, p.get_one))
  gc()
  out <- out[frac_with_data_ew > 0] # keep periods with data
  rm(vv)

  # add CSM names, save
  tmp <- unique(out[, .(var)])
  tmp[, var_lab := c("Industry momentum", "BEA customer", "BEA supplier", "Technology link", "Geographic link", "Complicated firms", "Economic link", "Analyst coverage")]
  out <- merge(out, tmp, by = "var")
  rm(tmp)
  out_dir <- paste0("tmp/processed_signals/", stock_base)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  saveRDS(out, paste0(out_dir, "/signal_availability.RDS"))
  toc()
}
