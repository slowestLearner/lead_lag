# -- Separate 1m CSM profitability by 3x3 liquidity bins
library(sandwich)
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")


# -- get split signals
# this_type <- "signal_by_liquidity_within_row_of_sub_matrices"
# this_type <- "signal_by_liquidity_within_row_of_pi_matrix"
this_type <- "signal_by_liquidity"
if (.Platform$OS.type == "windows") {
  base_dir <- "D:/Dropbox/JD/"
} else {
  base_dir <- "~/Dropbox/JD/"
}

from_dir <- file.path(base_dir, "data", this_type, "liq_var_minus_spread/")
file_map <- c(
  "Analyst.Rds"     = "analyst",
  "BEAcustomer.Rds" = "beacustomer",
  "BEAsupplier.Rds" = "beasupplier",
  "Econ.Rds"        = "econ",
  "Geo.Rds"         = "geo",
  "Indu.Rds"        = "industry",
  "Pseudo.Rds"      = "pseudo",
  "Tec.Rds"         = "tech",
  "combined.Rds"    = "combined"
)

# --- 1) load next-month return data and split by liquidity terciles

tic("loading return data")
from_file <- file.path(base_dir, "data/Stocks", "Monthly_CRSP.RDS")
ret_data <- readRDS(from_file) %>%
  select(yyyymm, permno, ret) %>%
  na.omit() %>%
  mutate(mm = yyyymm - floor(yyyymm / 100) * 100) %>%
  mutate(yyyymm_prev = ifelse(mm == 1, yyyymm - 100 + 11, yyyymm - 1)) %>%
  select(-mm)

ret_data <- merge(ret_data, ret_data[, .(yyyymm = yyyymm_prev, permno, ret1 = ret)],
  by = c("yyyymm", "permno"), all.x = T
) %>%
  filter(yyyymm <= 202312) %>%
  select(-ret, -yyyymm_prev) %>%
  mutate(ret1 = ifelse(!is.na(ret1), ret1, 0)) %>%
  group_by(yyyymm) %>%
  mutate(ret1 = DescTools::Winsorize(ret1, quantile(ret1, probs = c(0.001, 0.999), na.rm = TRUE))) %>%
  ungroup() %>%
  setDT()
gc()

# NEW: demeand ret1 by period... should make it more stable
ret_data[, ret1 := ret1 - mean(ret1), .(yyyymm)]

# separate by target stock into terciles as well
from_file <- file.path(base_dir, "formal_tests/tmp/asy/measuring_arb_cost/fitted_measures/put_together.RDS")
liq_data <- readRDS(from_file) %>%
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
setkey(ret_data, yyyymm, permno)
gc()
toc()

# --- 2) loop through individual CSM. Compute 3x3 portfolio results. Later can consider parallelizing
# current each CSM takes 10-20 secs
for (f_name in names(file_map)) {
  # f_name <- names(file_map)[1]
  tic(f_name)

  # read data
  f_path <- file.path(from_dir, f_name)
  data <- readRDS(f_path)[, var := file_map[[f_name]]]

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

  # parse var_type_bin -> (var_type, liq_bin)
  tmp <- unique(data[, .(var_type_bin)]) %>%
    mutate(var_type_bin = as.character(var_type_bin)) %>%
    mutate(var_type = ifelse(grepl("_total_", var_type_bin), "total", ifelse(grepl("_s_", var_type_bin), "sym", "asy"))) %>%
    mutate(liq_bin = as.integer(substr(var_type_bin, nchar(var_type_bin), nchar(var_type_bin))))
  data <- tmp[data, on = "var_type_bin"][, var_type_bin := NULL]
  rm(tmp)

  # merge with returns and form portfolio
  setkey(data, yyyymm, permno)
  data <- ret_data[data, nomatch = 0]
  setnames(data, "liq_bin", "source_liq_bin") # separate source and target liq bins
  gc()

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

  to_dir <- paste0("tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/", this_type, "/1m_ret_by_3x3_not_scaled/")
  dir.create(to_dir, recursive = T, showWarnings = F)
  saveRDS(out, paste0(to_dir, file_map[[f_name]], ".RDS"))


  toc()
}
