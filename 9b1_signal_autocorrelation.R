# --- compute autocorrelation of signals
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")


for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
  tic(stock_base)
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
  data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  data <- data[, .(yyyymm, permno, var, signal = signal_s + signal_a)]

  # append the combined signal
  tmp <- readRDS(paste0("tmp/processed_signals/", stock_base, "/combined_signal.RDS"))[, .(yyyymm, permno, var = "combined", signal)]
  data <- rbind(data, tmp)
  rm(tmp)

  # scale them to have 100% exposure on each side (should matter very little)
  data[, dir := sign(signal)]
  data[, sum_abs_signal := sum(abs(signal)), by = .(yyyymm, var, dir)]
  data[, signal := signal / sum_abs_signal][, sum_abs_signal := NULL]

  # get one lag
  data[, idx := frank(yyyymm, ties.method = "dense")]
  data <- merge(data, data[, .(idx = idx + 1, permno, var, signal_1 = signal)], by = c("idx", "permno", "var"))
  gc()

  # estimate autocorrelations
  data_list <- split(data, by = "var")

  p.get_one <- function(this_data) {
    # this_data <- data_list[[1]]
    this_data[, fe := 1]
    out <- data.table()

    mm <- feols(signal ~ signal_1 | fe, this_data, cluster = ~ yyyymm + permno)
    out <- rbind(out, data.table(spec_idx = 1, fe_type = "none", coef = mm$coefficients[1], se = diag(vcov(mm))))

    mm <- feols(signal ~ signal_1 | yyyymm, this_data, cluster = ~ yyyymm + permno)
    out <- rbind(out, data.table(spec_idx = 2, fe_type = "time FE", coef = mm$coefficients[1], se = diag(vcov(mm))[1]))

    mm <- feols(signal ~ signal_1 | yyyymm + permno, this_data, cluster = ~ yyyymm + permno)
    out <- rbind(out, data.table(spec_idx = 3, fe_type = "time + stock FE", coef = mm$coefficients[1], se = diag(vcov(mm))[1]))

    out[, var := this_data[1, var]]
    return(out)
  }

  out <- rbindlist(lapply(data_list, p.get_one))


  to_file <- paste0("tmp/processed_signals/", stock_base, "/signal_autocorrelation.RDS")
  dir.create(dirname(to_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(out, to_file)
  toc()
}
