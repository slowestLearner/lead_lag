# dsimilar to 2a1, but use the superset of controls in the literature
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

# -- get signals


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

  setnames(data, "signal_s", "signal_sym")
  setnames(data, "signal_a", "signal_asy")
  data[, signal := signal_sym + signal_asy]
  data[, c("signal_sym", "signal_asy") := NULL]


  # combined signal = average over the rest
  vars_per_month <- data[, .(n_total_vars = uniqueN(var)), by = yyyymm]

  data[vars_per_month, on = "yyyymm", N_denom := i.n_total_vars]

  data_combined <- data[, .(
    var = "combined",
    signal = sum(signal, na.rm = TRUE) / N_denom[1]
  ), by = .(yyyymm, permno)]
  data[, N_denom := NULL]

  # data_combined <- data[, .(var = "combined", signal = mean(signal)), .(yyyymm, permno)]
  data <- rbind(data, data_combined)
  rm(data_combined)

  # get superset of the fm controls used in the literature
  control_literature <- readRDS(file = "../../data/Stocks/Characteristics/signed_predictors_all_wide.Rds") %>%
    dplyr::select(permno, yyyymm, AssetGrowth, BM, GP, Mom12m, RD, STreversal, Size) %>%
    setDT()

  ret_data <- readRDS("../../data/Stocks/Monthly_CRSP.RDS") %>%
    dplyr::transmute(yyyymm, permno, Turnover = vol / shrout)
  setDT(ret_data)

  super_set <- ret_data[control_literature, on = .(yyyymm, permno)]
  rm(control_literature, ret_data)
  gc()


  vars_super_set <- setdiff(names(super_set), c("yyyymm", "permno"))


  calc_centered_rank <- function(x) {
    out <- numeric(length(x))
    valid_idx <- !is.na(x)
    n <- sum(valid_idx)

    # Apply logic based on count of valid observations
    if (n > 1) {
      # Rank only the valid observations
      r <- frank(x[valid_idx], ties.method = "first")

      # Scale to [-0.5, 0.5]
      out[valid_idx] <- (r - 1) / (n - 1) - 0.5
    } else if (n == 1) {
      out[valid_idx] <- 0
    }
    return(out)
  }


  cols_to_transform <- vars_super_set

  # We use lapply(.SD) to apply the function to multiple columns at once
  super_set[, (cols_to_transform) := lapply(.SD, calc_centered_rank),
    by = .(yyyymm),
    .SDcols = cols_to_transform
  ]



  # merge all together, fill zeros
  data <- data %>%
    left_join(super_set, by = c("yyyymm", "permno"))

  data[is.na(data)] <- 0

  toc()

  f_string <- paste0("signal ~ ", paste0(vars_super_set, collapse = " + "))
  my_formula <- as.formula(f_string)

  tic("Fast Regression Calculation")

  data <- data[,
    {
      resid_values <- tryCatch(
        {
          mod <- RcppArmadillo::fastLm(my_formula, data = .SD)
          mod$residuals
        },
        error = function(e) {
          rep(NA_real_, .N)
        }
      )
      .(permno = permno, signal_Super_set = resid_values)
    },
    by = .(yyyymm, var)
  ]

  toc()


  # save
  to_dir <- file.path("tmp/raw_data/signals/", stock_base, "total_signal/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  saveRDS(data, paste0(to_dir, "/fm_residualized_super_set.RDS"))
}
