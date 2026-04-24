# Set the group for computing the fraction of total CSM return in sym or asy, for 1 to 12 periods
# use NW to compute covariance, and do ratios in the next step
# NOTE: this code partially overlaps with an earlier script (I believe is 2b?).
# Would be good to find a way to combine
source("runmefirst.R")
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code


# different specifications
for (stock_base in c("all", "large")) {
  # stock_base <- 'all'
  from_dir_base <- paste0("tmp/portfolio_results/", stock_base, "/scale_by_total/")
  spec_data <- data.table(spec_dir = list.files(from_dir_base))

  # summarize returns over these horizons
  hor_data <- data.table(
    from_hor = 1,
    to_hor = 1:180
  ) %>% mutate(hor_idx = row_number())

  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/statistics/newey_west_fraction/")
  dir.create(to_dir, recursive = TRUE, showWarnings = FALSE)

  # Long-short portfolio returns
  for (this_spec in spec_data[, spec_dir]) {
    # this_spec <- spec_data[2, spec_dir]
    print(this_spec)
    data <- readRDS(paste0(from_dir_base, this_spec, "/returns.RDS"))

    # NOTE: here is a hack, but later need to fix upstream. Can never have -100% return
    data[, ret_fut := ifelse(ret_fut < -.99, -.99, ret_fut)]

    # also create a combined version
    data_combined <- data[, .(var = "combined", ret_fut = mean(ret_fut), sum_w_pos = mean(sum_w_pos), sum_w_neg = mean(sum_w_neg)), .(yyyymm, hor, weight_type, var_type, factor_model)]
    data_combined_first <- data_combined %>%
      filter(yyyymm <= 196212) %>%
      mutate(var = "combined_1926_1962")
    data_combined_second <- data_combined %>%
      filter(yyyymm > 196212) %>%
      mutate(var = "combined_1963_2023")
    data <- rbind(data, data_combined, data_combined_first, data_combined_second)
    rm(data_combined, data_combined_first, data_combined_second)
    gc()

    # compute cumulative returns by vintage
    tmp <- unique(data[, .(var, weight_type, var_type, factor_model)]) %>% mutate(spec_idx = row_number())
    data <- merge(data, tmp, by = c("var", "weight_type", "var_type", "factor_model")) %>%
      arrange(spec_idx, yyyymm, hor) %>%
      group_by(spec_idx, yyyymm) %>%
      # mutate(cumret = cumsum(log(1 + ret_fut))) %>% # log returns
      mutate(cumret = cumprod(1 + ret_fut) - 1) %>%
      ungroup() %>%
      setDT()
    rm(tmp)

    # add a zero for the beginning
    data_initial <- copy(data[hor == 1]) %>% mutate(hor = 0, cumret = 0)
    data <- rbind(data_initial, data)
    rm(data_initial)

    out_all <- data.table()
    for (this_hor_idx in hor_data[, hor_idx]) {
      # this_hor_idx = 1
      tic(paste0("this_spec = ", this_spec, ", this_hor_idx = ", this_hor_idx))

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

      # get spec_idx meaning
      this_data <- data %>%
        select(spec_idx, var, var_type, weight_type, factor_model) %>%
        unique() %>%
        merge(this_data, by = "spec_idx") %>%
        select(-spec_idx)

      # widen
      this_data <- dcast(this_data, yyyymm + var + weight_type + factor_model ~ var_type, value.var = "cumret")

      # helper function to compute NW s.e. for a specific horizon. also compute vol, which is useful for sharpe (1 period)
      p.get_one_case <- function(this_subdata) {
        # this_subdata <- this_data_list[[1]]


        nw_cov <- lrvar(as.matrix(this_subdata[, .(total, sym, asy)]), type = "Newey-West", prewhite = FALSE, adjust = TRUE, lag = hor_months)
        nw_cov <- as.data.table(nw_cov) %>%
          mutate("row_name" = c("total", "sym", "asy")) %>%
          data.table::melt(id.vars = "row_name", variable.name = "col_name", value.name = "cov") %>%
          mutate(
            var = this_subdata[1, var],
            weight_type = this_subdata[1, weight_type],
            factor_model = this_subdata[1, factor_model],
            hor = hor_months
          )

        return(nw_cov)
      }
      setDT(this_data)

      this_data_list <- split(this_data, by = c("var", "weight_type", "factor_model"))
      plan(multisession, workers = nc)
      out_list <- future_lapply(
        this_data_list,
        p.get_one_case,
        future.packages = c("sandwich", "dplyr", "data.table")
      )

      plan(sequential)
      out <- rbindlist(out_list)
      # out <- Reduce(rbind, mclapply(this_data_list, p.get_one_case, mc.cores = nc))

      out_all <- rbind(out_all, out)
      toc()
    }

    out_all[, spec := this_spec]
    saveRDS(out_all, paste0(to_dir, this_spec, ".RDS"))
  }
}
