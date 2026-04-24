# --- Let's produce a 3x3 table with standard errors
source("runmefirst.R")
library(sandwich)
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code

# 1m returns
# this_type <- "signal_by_liquidity_within_row_of_pi_matrix"
this_type <- "signal_by_liquidity"
data <- readRDS(paste0(
  "tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/",
  this_type, "/1m_ret_by_3x3_scaled.RDS"
)) %>%
  mutate(source_liq_bin = as.character(source_liq_bin)) %>%
  mutate(target_liq_bin = as.character(target_liq_bin)) %>%
  select(yyyymm, var, var_type, source_liq_bin, target_liq_bin, ret1)

# expand to have bin3 - bin1
data_by_target <- data %>%
  dcast(yyyymm + var + var_type + source_liq_bin ~ target_liq_bin, value.var = "ret1") %>%
  mutate(target_liq_bin = "3_1") %>%
  mutate(ret1 = `3` - `1`) %>%
  select(-`3`, -`2`, -`1`) %>%
  setDT()

data_by_source <- data %>%
  dcast(yyyymm + var + var_type + target_liq_bin ~ source_liq_bin, value.var = "ret1") %>%
  mutate(source_liq_bin = "3_1") %>%
  mutate(ret1 = `3` - `1`) %>%
  select(-`3`, -`2`, -`1`) %>%
  setDT()

data <- rbindlist(list(data, data_by_target, data_by_source), use.names = T)
rm(data_by_target, data_by_source)

# do standard errors  
out <- data[, .(mean_ret1 = mean(ret1), n = .N, se_ret1 = sd(ret1) / sqrt(length(yyyymm))), .(var, var_type, source_liq_bin, target_liq_bin)]

to_file <- paste0("tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/", this_type, "/1m_ret_by_3x3_scaled_summarized.RDS")
saveRDS(out, to_file)


out[, tstat := mean_ret1 / se_ret1]
dcast(out[var == "combined" & var_type == "total"], target_liq_bin ~ source_liq_bin, value.var = "mean_ret1")
dcast(out[var == "combined" & var_type == "total"], target_liq_bin ~ source_liq_bin, value.var = "tstat")
