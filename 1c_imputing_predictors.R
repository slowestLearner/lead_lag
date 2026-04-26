# --- Use the BvB approach to impute predictors
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
  print(paste("Processing stock_base:", stock_base))
  tt_stock_base <- Sys.time()

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
  data[, signal := signal_a + signal_s]
  data <- data[, .(yyyymm, permno, var, signal)] %>% na.omit()

  # for each (year-month, signal), require having 100 observations to use
  tt <- data[, list(obs = length(permno)), list(yyyymm, var)]
  tt <- tt[obs >= 100][, obs := NULL]
  data <- merge(data, tt, by = c("yyyymm", "var"))
  rm(tt, data_list, file_map, base_dir)

  # once a signal becomes available, fill in zeros for missing values
  tmp <- unique(data[, .(yyyymm, permno)])
  vv <- unique(data$var)
  out <- tmp[, .(yyyymm, permno, var = rep(vv, each = .N))]
  rm(tmp, vv)

  # figure out when a variable becomes available. after that, fill in zeros for missing values
  out <- merge(out, data[, .(first_ym = min(yyyymm)), var], by = "var")
  out <- out[yyyymm >= first_ym][, first_ym := NULL]
  data <- merge(out, data, by = c("yyyymm", "permno", "var"), all.x = T)
  data[is.na(signal), signal := 0]
  rm(out)

  # figure out the periods for backfilling
  tdata <- data[, list(first_ym = min(yyyymm), max_ym = max(yyyymm)), var]
  tdata <- tdata[order(first_ym)]

  # turn data into wide format
  data <- dcast(data, yyyymm + permno ~ var, value.var = "signal")
  gc()

  # impute backward, variable by variable
  tt <- unique(tdata[, list(first_ym)])[order(-first_ym)][, idx := .I] # each idx is an imputation step
  n <- nrow(tt)
  tdata <- merge(tdata, tt, by = "first_ym")
  tdata <- tdata[order(idx)]
  rm(tt)
  tdata <- tdata[order(idx)]

  for (this_idx in 1:(n - 1)) {
    print(paste0("this_idx = ", this_idx, ", filled data = ", mean(0 == rowSums(is.na(data)))))

    # impute data for yyyymm from cut1 to cut2
    cut1 <- tdata[idx == (this_idx + 1)][1, first_ym]
    cut2 <- tdata[idx == this_idx][1, first_ym]

    # use x_vars to impute y_vars
    y_vars <- tdata[idx <= this_idx, var]
    x_vars <- tdata[idx > this_idx, var]

    for (this_y in y_vars) {
      print(this_y)
      setnames(data, this_y, "y")

      # regression equation
      ff <- paste0("y ~ ", paste0(x_vars, collapse = "+"))

      # estimate regression and impute
      ols <- feols(as.formula(ff), data[yyyymm >= cut2])
      xx <- (data[, yyyymm] >= cut1) & (data[, yyyymm] < cut2)
      data[xx, y := predict(ols, data[xx])]

      setnames(data, "y", this_y)
    }
  }
  rm(cut1, cut2, ff)



  # # a few variables lack the last few periods. Let me fill forward for those
  # vv <- setdiff(names(data), c("yyyymm", "permno"))
  # y_vars <- names(data)[colSums(is.na(data)) > 0]
  # x_vars <- setdiff(vv, y_vars)
  # for (this_y in y_vars) {
  #   print(this_y)
  #   setnames(data, this_y, "y")

  #   # regression equation
  #   ff <- paste0("y ~ ", paste0(x_vars, collapse = "+"))

  #   # estimate regression and impute
  #   ols <- feols(as.formula(ff), data)
  #   xx <- data[, is.na(y)]
  #   data[xx, y := predict(ols, data[xx])]

  #   setnames(data, "y", this_y)
  # }
  # rm(this_y, y_vars, x_vars, vv, xx, ols, ff)

  stopifnot(1 == mean(0 == rowSums(is.na(data)))) # check if fully filled
  to_file <- paste0("tmp/processed_signals/", stock_base, "/signals_imputed.RDS")
  dir.create(dirname(to_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(data, to_file)
  print(paste("stock_base: ", stock_base, "Time taken:", round(as.numeric(Sys.time() - tt_stock_base) / 60, 2), "mins"))
}
