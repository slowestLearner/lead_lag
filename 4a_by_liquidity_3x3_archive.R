# -- Separate 1m CSM profitability by 3x3 liquidity bins
# results are weird when my signals are based on demeaned matrices. what about demeaning ret1 first?
# nevertheless, am giving it a try on the matrices that have 1/3 for each type
library(sandwich)
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")


# -- get split signals - wait, so scaling is off. NEED TO CHANGE SOMEHOW
# this_type <- "signal_by_liquidity_within_row_of_sub_matrices"
# this_type <- "signal_by_liquidity_within_row_of_pi_matrix"
this_type <- "signal_by_liquidity"
if (.Platform$OS.type == "windows") {
  base_dir <- "D:/Dropbox/JD/"
} else {
  base_dir <- "~/Dropbox/JD/"
}

from_dir <- file.path(base_dir, "data", this_type, "liq_var_minus_spread/")
# from_dir <-  paste0(base_dir, this_type, "/liq_var_minus_spread/")
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

# -- load data
tic("loading data")
data_list <- lapply(names(file_map), function(f_name) {
  f_path <- file.path(from_dir, f_name)

  if (file.exists(f_path)) {
    dt <- readRDS(f_path)
    dt[, var := file_map[[f_name]]]
    return(dt)
  } else {
    warning(paste("File missing:", f_path))
    return(NULL)
  }
})

data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
rm(data_list)
gc()
toc()

# # sanity check: mean-zero, right?
# data[, mean(signal_a_bin1 + signal_a_bin2 + signal_a_bin3), .(yyyymm, var)]
# data[, mean(signal_s_bin1 + signal_s_bin2 + signal_s_bin3), .(yyyymm, var)]

# construct total signal by adding up asy and sym
data[, signal_total_bin1 := signal_a_bin1 + signal_s_bin1]
data[, signal_total_bin2 := signal_a_bin2 + signal_s_bin2]
data[, signal_total_bin3 := signal_a_bin3 + signal_s_bin3]

# melt
data <- data.table::melt(data,
  id.vars = c("yyyymm", "permno", "var"),
  variable.name = "var_type_bin", value.name = "signal"
)
data[, var_type_bin := as.character(var_type_bin)]
gc()

# PAUSE
# parse var_type_bin -> (var_type, liq_bin) in place (no join / no temp copy)
type_map <- c(
  signal_a_bin1 = "asy", signal_a_bin2 = "asy", signal_a_bin3 = "asy",
  signal_s_bin1 = "sym", signal_s_bin2 = "sym", signal_s_bin3 = "sym",
  signal_total_bin1 = "total", signal_total_bin2 = "total", signal_total_bin3 = "total"
)
bin_map <- c(
  signal_a_bin1 = 1L, signal_a_bin2 = 2L, signal_a_bin3 = 3L,
  signal_s_bin1 = 1L, signal_s_bin2 = 2L, signal_s_bin3 = 3L,
  signal_total_bin1 = 1L, signal_total_bin2 = 2L, signal_total_bin3 = 3L
)
data[, var_type := unname(type_map[var_type_bin])]
data[, liq_bin := unname(bin_map[var_type_bin])]
data[, var_type_bin := NULL]
gc()

# # original version, more memory intensive (kept for reference)
# tmp <- unique(data[, .(var_type_bin)]) %>%
#   mutate(var_type_bin = as.character(var_type_bin)) %>%
#   mutate(var_type = ifelse(grepl("_total_", var_type_bin), "total", ifelse(grepl("_s_", var_type_bin), "sym", "asy"))) %>%
#   mutate(liq_bin = as.integer(substr(var_type_bin, nchar(var_type_bin), nchar(var_type_bin))))
# data <- tmp[data, on = "var_type_bin"][, var_type_bin := NULL]
# rm(tmp)

# summarize to get overall signal - NOTE: if we sort liquidity by signal then this is a bit complex
# NOTE: this is really slow. Later can consider computing this before melting, which will be faster
combined_data <- data[, .(var = "combined", signal = mean(signal)), .(yyyymm, permno, var_type, liq_bin)]
data <- rbind(data, combined_data)
rm(combined_data)
gc()

# reduce memory footprint a bit?
data[, var := as.factor(var)]
data[, var_type := as.factor(var_type)]

# PAUSE

# merge with return data - make sure the same universe
from_dir <- file.path(base_dir, "data/Stocks", "Monthly_CRSP.RDS")

# ret_data <- readRDS("~/Dropbox/JD/data/Stocks/Monthly_CRSP.RDS") %>%
ret_data <- readRDS(from_dir) %>%
  select(yyyymm, permno, ret) %>%
  mutate(mm = yyyymm - floor(yyyymm / 100) * 100) %>%
  na.omit() %>%
  mutate(yyyymm_prev = ifelse(mm == 1, yyyymm - 100 + 11, yyyymm - 1)) %>%
  select(-mm)

ret_data <- merge(ret_data, ret_data[, .(yyyymm = yyyymm_prev, permno, ret1 = ret)],
  by = c("yyyymm", "permno"), all.x = T
) %>%
  filter(yyyymm <= 202312) %>%
  select(-ret, -yyyymm_prev) %>%
  mutate(ret1 = ifelse(!is.na(ret1), ret1, 0)) %>%
  group_by(yyyymm) %>%
  mutate(ret1 = Winsorize(ret1, probs = c(0.001, 0.999), na.rm = TRUE)) %>%
  ungroup() %>%
  setDT()
gc()

# NEW: demeand ret1 by period... should make it more stable
ret_data[, ret1 := ret1 - mean(ret1), .(yyyymm)]

# separate by target stock into terciles as well
from_dir <- file.path(base_dir, "formal_tests/tmp/asy/measuring_arb_cost/fitted_measures/put_together.RDS")
# liq_data <- readRDS("~/Dropbox/JD/formal_tests/tmp/asy/measuring_arb_cost/fitted_measures/put_together.RDS") %>%
liq_data <- readRDS(from_dir) %>%
  mutate(minus_spread = -exp(logspreadhat)) %>%
  select(yyyymm, permno, minus_spread) %>%
  na.omit()
ret_data <- merge(ret_data, liq_data, by = c("yyyymm", "permno"), all.x = T)
rm(liq_data)
gc()

# split into liquidity bins
ret_data <- ret_data %>%
  group_by(yyyymm) %>%
  mutate(target_liq_bin = ntile(minus_spread, 3)) %>%
  select(-minus_spread) %>%
  setDT()
gc()

# merge together and summarize by portfolio
setDT(data)
setDT(ret_data)
setkey(data, yyyymm, permno)
setkey(ret_data, yyyymm, permno)
data <- ret_data[data, nomatch = 0]
# data <- merge(data, ret_data, by = c("yyyymm", "permno"))
gc()
rm(ret_data)
setnames(data, "liq_bin", "source_liq_bin")

# # -- sanity: the signals have high correlation but just not the same means?
# out <- data[, .(signal = sum(signal)), .(yyyymm, permno, var, var_type)]
# tmp <- readRDS("~/Dropbox/JD/data/signal_demean/Analyst.Rds") %>% mutate(var = 'analyst', sym = signal_s,
#                                                                          asy = signal_a, total = signal_s + signal_a) %>%
#   select(yyyymm, permno, var, sym, asy, total) %>%
#   melt(id.vars = c('yyyymm','permno','var'), variable.name = 'var_type', value.name = 'signal_old') %>%
#   mutate(var_type = as.character(var_type))
# out <- merge(out, tmp, by = c('yyyymm','permno','var','var_type')); rm(tmp); gc()

# summarize portfolio
out <- data[, .(
  n_permno = .N, ret1 = sum(ret1 * signal),
  sum_signal_pos = sum(signal * (signal > 0)),
  sum_signal_neg = sum(signal * (signal <= 0))
), .(yyyymm, var, var_type, source_liq_bin, target_liq_bin)]
gc()

to_dir <- paste0("tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/", this_type, "/")
dir.create(to_dir, recursive = T, showWarnings = F)
saveRDS(out, paste0(to_dir, "1m_ret_by_3x3_not_scaled.RDS"))

# --- examine. We need to scale it

out_all <- readRDS(paste0(
  "tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/",
  this_type, "/1m_ret_by_3x3_not_scaled.RDS"
))
scale_data <- readRDS("tmp/portfolio_results/scale_by_total/remove_20pct_stk_FALSE/scaling.RDS") %>%
  filter(hor == 0) %>%
  select(-hor)

# scale_data <- readRDS("tmp/portfolio_results/scale_by_total_different_source_ret/remove_20pct_stk_FALSE/scaling.RDS") %>%
#   filter(hor == 0) %>%
#   select(-hor)
scale_data <- rbind(scale_data, scale_data[, .(var = "combined", sum_of_abs_signal_total = mean(sum_of_abs_signal_total)), yyyymm])
out_all <- merge(out_all, scale_data, by = c("yyyymm", "var"))
rm(scale_data)

out_all <- out_all %>%
  mutate(
    ret1 = ret1 / (sum_of_abs_signal_total / 2),
    sum_signal_pos = sum_signal_pos / (sum_of_abs_signal_total / 2),
    sum_signal_neg = sum_signal_neg / (sum_of_abs_signal_total / 2)
  ) %>%
  select(-sum_of_abs_signal_total)

# dcast(out_all[(var == 'combined') & (var_type == 'asy'), mean(ret1), .(source_liq_bin, target_liq_bin)],
#       target_liq_bin ~ source_liq_bin)

# # does it match? Not fully... need to fix later. For pseudo, or econ, is quite bad
# out <- out_all[, .(ret1_new = sum(ret1)), .(yyyymm, var, var_type)]
# tt <- readRDS('tmp/portfolio_results/scale_by_total/remove_20pct_stk_FALSE/returns.RDS') %>% filter(hor == 1) %>%
#   select(yyyymm, var, var_type, ret1_old = ret_fut)
# out <- merge(out, tt, by = c('yyyymm','var','var_type')); rm(tt)
# out[, cor(ret1_new, ret1_old), .(var, var_type)]

out <- copy(out_all[var == "combined"])
out <- out[, .(
  ret1 = round(100 * mean(ret1), 2),
  sum_abs_signal = mean(sum_signal_pos - sum_signal_neg)
), .(var_type, source_liq_bin, target_liq_bin)]
out[, ret1_per_signal_size := round(ret1 / sum_abs_signal, 2)]

this_var_type <- "total"
this_var_type <- "sym"
this_var_type <- "asy"

# dcast(out[var_type == this_var_type], target_liq_bin ~ source_liq_bin, value.var = "ret1_per_signal_size")
dcast(out[var_type == this_var_type], target_liq_bin ~ source_liq_bin, value.var = "ret1")

# do these add up? These are a bit high, no... so perhaps scaling is wrong
out[, sum(ret1), var_type]
dcast(out[var_type == this_var_type], target_liq_bin ~ source_liq_bin, value.var = "sum_abs_signal")
