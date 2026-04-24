# Compute Newey-West t-statistics for cumulative L/S returns. Summarize over horizons of (m -> n) months
source("runmefirst.R")
library(this.path)
library(data.table)
library(dplyr)
library(future.apply)
library(sandwich)

setwd(this.path::this.dir())

# different specifications
for (stock_base in c("all", "large")) {
  print(stock_base)
  
  from_dir_base <- paste0("tmp/portfolio_results/", stock_base, "/cf_dr/")
  
  # -- summarize returns over these horizons
  hor_data <- data.table(
    from_hor = c(1, 1, 13, 13, 13),
    to_hor = c(1, 12, 60, 84, 120)
  ) # these are for the table
  
  # these are for the plots
  hor_data <- rbind(hor_data, data.table(
    from_hor = 1,
    to_hor = 1:180
  )) %>%
    unique() %>%
    mutate(hor_idx = row_number())
  
  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/statistics/cf_dr/")
  dir.create(to_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Long-short portfolio returns
  data <- readRDS(paste0(from_dir_base, "returns.RDS"))
  
  # Create subsamples for the combined version
  data_combined <- data[var == "combined"]
  data_combined_first <- data_combined %>%
    filter(yyyymm <= 196212) %>%
    mutate(var = "combined_1926_1962")
  data_combined_second <- data_combined %>%
    filter(yyyymm > 196212) %>%
    mutate(var = "combined_1963_2023")
  data <- rbind(data, data_combined_first, data_combined_second)
  rm(data_combined, data_combined_first, data_combined_second)
  gc()
  
  # ============================================================================
  # CRITICAL CHANGE: 
  # Since the data is ALREADY cumulative from t to t+h, we DO NOT cumsum().
  # We just rename ret_fut to cumret.
  # ============================================================================
  tmp <- unique(data[, .(var, weight_type, var_type, factor_model)]) %>% 
    mutate(spec_idx = row_number())
  
  data <- merge(data, tmp, by = c("var", "weight_type", "var_type", "factor_model")) %>%
    arrange(spec_idx, yyyymm, hor) %>%
    # mutate(cumret = cumsum(ret_fut)) %>%  <-- REMOVED
    mutate(cumret = ret_fut) %>%          # <-- NEW: It's already cumulative
    setDT()
  rm(tmp)
  
  # add a zero for the beginning (hor = 0)
  data_initial <- copy(data[hor == 1]) %>% 
    mutate(hor = 0, cumret = 0)
  data <- rbind(data_initial, data)
  rm(data_initial)
  
  # loop over horizons
  out_all <- data.table()
  
  for (this_hor_idx in hor_data[, hor_idx]) {
    tic(paste0("hor_idx = ", this_hor_idx))
    
    # parse
    this_from_hor <- hor_data[hor_idx == this_hor_idx, from_hor]
    this_to_hor <- hor_data[hor_idx == this_hor_idx, to_hor]
    hor_months <- this_to_hor - this_from_hor + 1
    
    # merge data
    from_data <- copy(data[hor == (this_from_hor - 1)])
    to_data <- copy(data[hor == this_to_hor])
    
    this_data <- merge(
      from_data[, .(yyyymm, spec_idx, ret_short = cumret)],
      to_data[, .(yyyymm, spec_idx, ret_long = cumret)],
      by = c("yyyymm", "spec_idx")
    ) %>%
      # Simple subtraction works perfectly for CF and DR identity:
      # (News from t to 60) - (News from t to 12) = News from 13 to 60
      mutate(cumret = ret_long - ret_short) %>% 
      dplyr::select(-ret_long, -ret_short) %>%
      arrange(spec_idx, yyyymm) %>%
      setDT()
    
    # helper function to compute NW s.e. for a specific horizon. 
    p.get_one_spec <- function(this_spec_idx) {
      
      # Subset data once
      sub_data <- this_data[spec_idx == this_spec_idx]
      this_vol <- sd(sub_data$cumret, na.rm = TRUE)
      
      # Safety Check: If variance is 0, NA, or we have very few rows, NW will fail.
      if (is.na(this_vol) || this_vol == 0 || nrow(sub_data) < 3) {
        return(data.table(
          spec_idx = this_spec_idx,
          coef = mean(sub_data$cumret, na.rm = TRUE),
          vol = this_vol,
          se = NA_real_,
          n_periods = nrow(sub_data)
        ))
      }
      
      mm <- lm(cumret ~ 1, data = sub_data)
      
      # FIX: prewhite = FALSE is critical here to prevent the VAR(1) crash
      nw_cov <- NeweyWest(mm, lag = hor_months, prewhite = FALSE)
      
      return(data.table(
        spec_idx = this_spec_idx,
        coef = mm$coef[1],
        vol = this_vol,
        se = sqrt(diag(nw_cov))[1], # Extract the standard error
        n_periods = nrow(sub_data)
      ))
    }
    
    plan(multisession, workers = parallel::detectCores() - 1)
    
    results_list <- future_lapply(
      unique(this_data[, spec_idx]),
      p.get_one_spec,
      future.packages = c("sandwich", "data.table")
    )
    plan(sequential)
    
    out <- rbindlist(results_list) %>%
      mutate(tstat = coef / se) %>%
      setDT()
    
    # Get spec details back
    spec_details <- unique(data[, .(spec_idx, var, weight_type, var_type, factor_model)])
    out <- merge(out, spec_details, by = "spec_idx") %>% dplyr::select(-spec_idx)
    
    out[, hor_idx := this_hor_idx]
    out[, from_hor := this_from_hor]
    out[, to_hor := this_to_hor]
    
    dir.create(paste0(to_dir, "hor"), recursive = TRUE, showWarnings = FALSE)
    saveRDS(out, paste0(to_dir, "hor/", this_hor_idx, ".RDS"))
    toc()
  }
  
  files <- list.files(paste0(to_dir, "hor/"), full.names = TRUE)
  out_all <- rbindlist(lapply(files, readRDS))
  saveRDS(out_all, paste0(to_dir, "scale_by_total.RDS"))
  unlink(paste0(to_dir, "hor/"), recursive = TRUE)
}