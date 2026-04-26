# --- Project CSM signals onto characteristics and industries dummies in cross-sectional regressions to get residuals
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

# -- get signals
for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
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

  # Combine and focus on the total signal
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
  data <- rbind(data, data_combined)
  rm(data_combined)

  # get current stock characteristics
  char_data <- readRDS("../../data/Stocks/Characteristics/andrew_chen_characteristics.RDS") %>% setDT()

  # rank transform and then filled Na by 0
  vars_char <- setdiff(names(char_data), c("yyyymm", "permno"))

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


  cols_to_transform <- vars_char

  # We use lapply(.SD) to apply the function to multiple columns at once
  char_data[, (cols_to_transform) := lapply(.SD, calc_centered_rank),
    by = .(yyyymm),
    .SDcols = cols_to_transform
  ]

  # industries
  ind_data <- readRDS("../../data/Stocks/Characteristics/ff12_industries.RDS") %>% setDT()
  ind_data[is.na(ind_data)] <- 0
  vars_ind <- setdiff(names(ind_data), c("yyyymm", "permno"))

  # merge all together, fill zeros (takes some time)
  data <- data %>%
    left_join(char_data, by = c("yyyymm", "permno")) %>%
    left_join(ind_data, by = c("yyyymm", "permno"))

  data[is.na(data)] <- 0
  rm(char_data, ind_data)
  toc()

  # different regression models
  spec_data <- data.table(model_name = c("FF3", "FF3_ind", "FF3_ind_char")) %>%
    mutate(spec_idx = row_number()) %>%
    setDT()
  spec_data[spec_idx == 1, model_formula := "signal ~ beta + size + bm"]
  spec_data[spec_idx == 2, model_formula := paste0("signal ~ beta + size + bm + ", paste0(vars_ind, collapse = " + "))]
  spec_data[spec_idx == 3, model_formula := paste0("signal ~ ", paste0(c(vars_char, vars_ind), collapse = " + "))]

  # fit models by period
  data_list <- split(data, by = c("yyyymm", "var"))
  rm(data) # free memory
  gc()

  # helper function to estimate one residualized signal
  p.residualize_one <- function(this_data) {
    # this_data <- data_list[[1]]
    for (this_spec in spec_data[, spec_idx]) {
      # this_spec = 1
      # print(this_spec)
      this_model_formula <- spec_data[spec_idx == this_spec, model_formula]
      mm <- lm(as.formula(this_model_formula), this_data)
      this_data[, paste0("signal_", spec_data[spec_idx == this_spec, model_name]) := residuals(mm)]
    }

    vv <- names(this_data)
    this_data <- this_data[, c("yyyymm", "permno", "var", vv[grepl("signal", vv)]), with = FALSE]

    return(this_data)
  }

  # process in blocks. Takes around 3 mins
  processing <- data.table(idx = 1:length(data_list))
  block_size <- 100
  processing[, block_idx := ceiling(idx / block_size)]

  plan(multisession, workers = nc)
  out <- data.table()
  for (this_block in 1:max(processing[, block_idx])) {
    tic(paste0("Processing block ", this_block, " of ", max(processing[, block_idx])))
    # Get the specific subset of the list for this block
    current_batch_indices <- processing[block_idx == this_block, idx]
    current_batch_data <- data_list[current_batch_indices]
    res_list <- future_lapply(current_batch_data,
      p.residualize_one,
      future.seed = TRUE
    )

    out <- rbind(out, rbindlist(res_list))
    # out <- rbind(out, rbindlist(mclapply(data_list[processing[block_idx == this_block, idx]],
    #                                      p.residualize_one, mc.cores = nc)))
    toc()
  }
  gc()
  plan(sequential)

  # save
  to_dir <- file.path("tmp/raw_data/signals/", stock_base, "total_signal/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  saveRDS(out, paste0(to_dir, "/fm_residualized.RDS"))
}









# # --- sanity check
# # source('~/.runmefirst')

# # original signals
# tic("loading data")
# data <- readRDS("~/Dropbox/JD/data/signal_demean/Analyst.Rds")[, var := "analyst"]
# data <- rbind(data, readRDS("~/Dropbox/JD/data/signal_demean/BEAcustomer.Rds")[, var := "beacustomer"])
# data <- rbind(data, readRDS("~/Dropbox/JD/data/signal_demean/BEAsupplier.Rds")[, var := "beasupplier"])
# data <- rbind(data, readRDS("~/Dropbox/JD/data/signal_demean/Econ.Rds")[, var := "econ"])
# data <- rbind(data, readRDS("~/Dropbox/JD/data/signal_demean/Geo.Rds")[, var := "geo"])
# data <- rbind(data, readRDS("~/Dropbox/JD/data/signal_demean/Indu.Rds")[, var := "industry"])
# data <- rbind(data, readRDS("~/Dropbox/JD/data/signal_demean/Pseudo.Rds")[, var := "pseudo"])
# data <- rbind(data, readRDS("~/Dropbox/JD/data/signal_demean/Tec.Rds")[, var := "tech"])
# setnames(data, "signal_s", "signal_sym")
# setnames(data, "signal_a", "signal_asy")
# data[, signal_original := signal_sym + signal_asy]
# data[, c("signal_sym", "signal_asy") := NULL]
# toc()

# # residualized signals
# tmp <- readRDS("tmp/raw_data/signals/total_signal/fm_residualized.RDS") %>%
#   melt(id.vars = c("yyyymm", "permno", "var"), variable.name = "model_name", value.name = "signal_resid") %>%
#   setDT()
# data <- merge(data, tmp, by = c("yyyymm", "permno", "var"))
# rm(tmp)
# gc()

# data[, cor(signal_original, signal_resid), .(model_name, yyyymm, var)][, mean(V1), model_name]
