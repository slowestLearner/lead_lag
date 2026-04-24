# Compute Newey-West t-statistics for cumulative L/S returns. Summarize over horizons of (m -> n) months
source("runmefirst.R")
library(this.path)
setwd(this.path::this.dir())

# different specifications
for (stock_base in c("all", "large")) {
  # stock_base <- 'all'
  print(stock_base)

  from_dir_base <- paste0("tmp/portfolio_results/", stock_base, "/scale_by_total/")

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

  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/statistics/newey_west/")
  dir.create(to_dir, recursive = TRUE, showWarnings = FALSE)

  # Long-short portfolio returns
  data <- readRDS(paste0(from_dir_base, "/returns.RDS"))

  # # Create a combined version - TODO: already have a combined version!
  # data_combined <- data[
  #   , .(
  #     var = "combined", ret_fut = mean(ret_fut),
  #     sum_w_pos = mean(sum_w_pos),
  #     sum_w_neg = mean(sum_w_neg)
  #   ),
  #   .(yyyymm, hor, weight_type, var_type, factor_model)
  # ]

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
  # compute cumulative returns by vintage
  tmp <- unique(data[, .(var, weight_type, var_type, factor_model)]) %>% mutate(spec_idx = row_number())
  data <- merge(data, tmp, by = c("var", "weight_type", "var_type", "factor_model")) %>%
    arrange(spec_idx, yyyymm, hor) %>%
    group_by(spec_idx, yyyymm) %>%
    mutate(cumret = cumprod(1 + ret_fut) - 1) %>%
    ungroup() %>%
    setDT()
  rm(tmp)

  # add a zero for the beginning
  data_initial <- copy(data[hor == 1]) %>% mutate(hor = 0, cumret = 0)
  data <- rbind(data_initial, data)
  rm(data_initial)

  # for (this_spec in spec_data[, spec_dir]) {
  # loop over horizons
  out_all <- data.table()
  for (this_hor_idx in hor_data[, hor_idx]) {
    tic(paste0("hor_idx = ", this_hor_idx))

    # this_hor_idx <- 1
    # print(this_hor_idx)

    # parse
    this_from_hor <- hor_data[hor_idx == this_hor_idx, from_hor]
    this_to_hor <- hor_data[hor_idx == this_hor_idx, to_hor]
    hor_months <- this_to_hor - this_from_hor + 1

    # merge data
    from_data <- copy(data[hor == (this_from_hor - 1)])
    to_data <- copy(data[hor == this_to_hor])
    this_data <- merge(from_data[, .(yyyymm, spec_idx, ret_short = cumret)],
      to_data[, .(yyyymm, spec_idx, ret_long = cumret)],
      by = c("yyyymm", "spec_idx")
    ) %>%
      # mutate(cumret = ret_long - ret_short) %>% # log returns
      mutate(cumret = (1 + ret_long) / (1 + ret_short) - 1) %>%
      select(-ret_long, -ret_short) %>%
      arrange(spec_idx, yyyymm)

    # helper function to compute NW s.e. for a specific horizon. also compute vol, which is useful for sharpe (1 period)
    p.get_one_spec <- function(this_spec_idx) {
      mm <- lm(cumret ~ 1, this_data[spec_idx == this_spec_idx])
      return(data.table(
        spec_idx = this_spec_idx,
        coef = mm$coef[1],
        vol = this_data[spec_idx == this_spec_idx, sd(cumret)],
        se = sqrt(diag(NeweyWest(mm, lag = hor_months))),
        n_periods = nrow(this_data[spec_idx == this_spec_idx])
      ))
    }

    plan(multisession, workers = nc)

    results_list <- future_lapply(
      unique(this_data[, spec_idx]),
      p.get_one_spec,
      future.packages = c("sandwich", "data.table")
    )
    plan(sequential)

    out <- rbindlist(results_list) %>%
      mutate(tstat = coef / se)

    out <- merge(out, unique(data[, .(spec_idx, var, weight_type, var_type, factor_model)]), by = "spec_idx") %>% select(-spec_idx)
    out[, hor_idx := this_hor_idx]
    out[, from_hor := this_from_hor]
    out[, to_hor := this_to_hor]

    out_all <- rbind(out_all, out)
    toc()
  }
  # out_all[, spec := this_spec]
  saveRDS(out_all, paste0(to_dir, "scale_by_total.RDS"))
  # }
}

# # --- sanity: compare with earlier
# # source('runmefirst.R')

# data_new <- readRDS("tmp/portfolio_results/all/statistics/newey_west/scale_by_total.RDS")
# data_old <- readRDS("tmp/portfolio_results/all/statistics/newey_west/remove_20pct_stk_FALSE.RDS")[, spec := NULL]
# dim(data_new) == dim(data_old)
# data <- merge(data_new, data_old, by = c("var", "weight_type", "var_type", "factor_model", "hor_idx", "from_hor", "to_hor"))
# nrow(data) == nrow(data_new)

# # a few have changed
# data[, cor(coef.x, coef.y), var]
# data[var == "combined" & hor_idx %in% 1:3, .(var_type, hor_idx, from_hor, to_hor, coef.x, coef.y)]
# data[var == "combined_1926_1962" & hor_idx %in% 1:3, .(var_type, hor_idx, from_hor, to_hor, coef.x, coef.y)]
# data[var == "combined_1963_2023" & hor_idx %in% 1:3, .(var_type, hor_idx, from_hor, to_hor, coef.x, coef.y)]
