# --- Computes Newey-West means/t-stats of cumulative CF/raw portfolio returns by horizon
library(this.path)
setwd(this.path::this.dir())
source("../runmefirst.R")
options(width = 120)

# Define the same horizons used in 3b1
fill_horizons <- c(1, 2, 4, 8, 20, 40)

for (stock_base in c("all", "large")) {
  # stock_base <- 'all'
  print(paste("Calculating Statistics for stock_base:", stock_base))

  for (h_limit in fill_horizons) {
    # h_limit <- 40
    print(paste("  Processing Fill-Forward Horizon:", h_limit))

    # Input path from 3b1
    input_file <- paste0("tmp/portfolio_results/", stock_base, "/h", h_limit, "/returns.RDS")

    if (!file.exists(input_file)) {
      warning(paste("File not found:", input_file))
      next
    }

    data <- readRDS(input_file)

    # ============================================================================
    # SUBSAMPLE LOGIC for "combined" signal
    # ============================================================================
    data_combined <- data[var == "combined"]

    # Early sample: 1926 - 1962
    data_combined_first <- copy(data_combined[yyyymm <= 196212])[, var := "combined_1926_1962"]

    # Late sample: 1963 - 2023
    data_combined_second <- copy(data_combined[yyyymm > 196212])[, var := "combined_1963_2023"]

    # Re-bind to main dataset
    data <- rbind(data, data_combined_first, data_combined_second)
    rm(data_combined, data_combined_first, data_combined_second)
    gc()

    # ============================================================================
    # REMOVED REDUNDANT CUMSUM BLOCK
    # ============================================================================
    # Since both factor_model == "cf" and factor_model == "raw_ret" are now
    # natively processed as cumulative holding returns directly inside 2a4 and 2b4,
    # running cumsum() here is no longer needed. We preserve chronological sorting.
    setorder(data, factor_model, var, var_type, yyyymm, hor)

    # ============================================================================
    # ANCHOR AT T=0
    # ============================================================================
    # Create an anchor at horizon 0 with zero returns for every unique specification
    data_initial <- unique(data[, .(var, var_type, factor_model, yyyymm)]) %>%
      mutate(hor = 0, ret_fut = 0)
    data <- rbind(data, data_initial, fill = TRUE) %>% setDT()

    # Identify unique specifications to loop over
    specs <- unique(data[, .(var, var_type, factor_model)])

    # ============================================================================
    # STATISTICAL CALCULATION (NW T-STATS WITH OVERLAPPING CUMULATIVE LAGS)
    # ============================================================================
    p.get_stats <- function(i, data, specs) {
      this_spec <- specs[i]

      # Filter for the specific signal / model combination
      sub_data <- data[var == this_spec$var &
        var_type == this_spec$var_type &
        factor_model == this_spec$factor_model]

      # Loop through every monthly horizon present in the data (-60 to +120)
      res_list <- lapply(unique(sub_data$hor), function(h) {
        print(h)
        h_data <- sub_data[hor == h]

        # Security check: need enough periods for a regression
        if (nrow(h_data) < 10) {
          return(NULL)
        }

        mm <- lm(ret_fut ~ 1, data = h_data)

        # Since both 'cf' and 'raw_ret' represent overlapping horizons,
        # we adjust Newey-West lags using the absolute value of the month index.
        nw_lag <- max(abs(h), 1)

        # prewhite = FALSE prevents VAR(1) covariance crashes in brief historical regimes
        nw_se <- sqrt(diag(NeweyWest(mm, lag = nw_lag, prewhite = FALSE)))[1]

        data.table(
          hor = h,
          coef = coef(mm)[1],
          se = nw_se,
          tstat = coef(mm)[1] / nw_se,
          n_obs = nrow(h_data)
        )
      })

      res <- rbindlist(res_list)
      return(cbind(this_spec, res))
    }

    # Parallel execution across specifications
    nc <- parallel::detectCores()
    plan(multisession, workers = nc - 1)

    final_stats <- rbindlist(future_lapply(
      1:nrow(specs),
      p.get_stats,
      data = data,
      specs = specs,
      future.packages = c("data.table", "sandwich"),
      future.seed = TRUE
    ))

    plan(sequential)

    # Tag with fill_h for sensitivity plotting
    final_stats[, fill_h := h_limit]

    # Save results
    to_dir <- paste0("tmp/portfolio_results/", stock_base, "/statistics/cf_dr/")
    dir.create(to_dir, recursive = TRUE, showWarnings = FALSE)
    saveRDS(final_stats, paste0(to_dir, "scale_h", h_limit, ".RDS"))
  }
}
print("Statistical pipeline finished successfully. All asset horizons are aligned.")
