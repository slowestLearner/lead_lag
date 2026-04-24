# --- Do a scaled version and save
# IMPORTANT: this script involves another round of "ad hoc scaling". This can't be the final version. We need to have the code air-tight
source("runmefirst.R")
library(sandwich)
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code

# this_type <- "signal_by_liquidity_within_row_of_sub_matrices"
# this_type <- "signal_by_liquidity_within_row_of_pi_matrix"
this_type <- "signal_by_liquidity"

# results before scaling
from_dir <- paste0("tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/", this_type, "/1m_ret_by_3x3_not_scaled")
files <- list.files(from_dir, full.names = TRUE)
out_all <- rbindlist(lapply(files, readRDS))
rm(from_dir, files)

# scaling
# scale_data <- readRDS("tmp/portfolio_results/scale_by_total/remove_20pct_stk_FALSE/scaling_partial.RDS") %>%
# scale_data <- readRDS("tmp/portfolio_results/scale_by_total/remove_20pct_stk_FALSE/scaling.RDS") %>%
scale_data <- readRDS("tmp/portfolio_results/large/scale_by_total/scaling.RDS") %>%
  filter(hor == 0) %>%
  select(-hor, -factor_model)
scale_data <- rbind(scale_data, scale_data[, .(var = "combined", sum_of_abs_signal_total = mean(sum_of_abs_signal_total)), yyyymm])
out_all <- merge(out_all, scale_data, by = c("yyyymm", "var"))
rm(scale_data)

# scale
out_all <- out_all %>%
  mutate(
    ret1 = ret1 / (sum_of_abs_signal_total / 2),
    sum_signal_pos = sum_signal_pos / (sum_of_abs_signal_total / 2),
    sum_signal_neg = sum_signal_neg / (sum_of_abs_signal_total / 2)
  ) %>%
  select(-sum_of_abs_signal_total)

# may need to scale it again
tmp <- out_all[var_type == "total", .(ret1 = sum(ret1)), .(yyyymm, var)] %>%
  group_by(var) %>%
  summarize(ret1_new = mean(ret1)) %>%
  setDT()

# tt <- readRDS("tmp/portfolio_results/scale_by_total/remove_20pct_stk_FALSE/returns.RDS") %>%
tt <- readRDS("tmp/portfolio_results/large/scale_by_total/returns.RDS") %>%
  filter(var_type == "total") %>%
  filter(hor == 1) %>%
  select(yyyymm, var, ret1 = ret_fut)

# tt_combined <- tt %>% filter(var == 'combined')
# group_by(yyyymm) %>%
# summarize(ret1 = mean(ret1)) %>%
# mutate(var = "combined") %>%
# group_by(var) %>%
# summarize(ret1 = mean(ret1)) %>%
# ungroup() %>%
# setDT()

tt <- tt %>%
  group_by(var) %>%
  summarize(ret1 = mean(ret1)) %>%
  ungroup() %>%
  setDT()
# rm(tt_combined)

tmp <- merge(tmp, tt, by = c("var"))
rm(tt)

tmp[, scale := ret1 / ret1_new]
out_all <- merge(out_all, tmp[, .(var, scale)], by = c("var")) %>%
  mutate(ret1 = ret1 * scale) %>%
  select(-scale)
rm(tmp)

# Let's save the scaled version
to_file <- paste0("tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/", this_type, "/1m_ret_by_3x3_scaled.RDS")
saveRDS(out_all, to_file)
