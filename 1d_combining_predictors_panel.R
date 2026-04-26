# --- Combining predictors using panel regression
# NOTE: rescale to give each time period equal weight
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")
options(width = 200)

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
  tic(stock_base)

  # get next-month stock returns
  data <- readRDS("../../data/Stocks/Monthly_CRSP.RDS")[, .(yyyymm, permno, ret)]
  data[, mm := yyyymm %% 100]
  data <- data[, .(yyyymm = fifelse(mm == 1, yyyymm - 100 + 11, yyyymm - 1), permno, ret1 = ret)] %>% na.omit()

  # imputed signals, put into z-scores for comparability
  tmp <- readRDS(paste0("tmp/processed_signals/", stock_base, "/signals_imputed.RDS"))
  tmp <- data.table::melt(tmp, id.vars = c("yyyymm", "permno"), variable.name = "var", value.name = "signal")
  tmp[, signal := Winsorize(signal, quantile(signal, probs = c(.001, .999), na.rm = T)), .(yyyymm, var)]
  tmp[, signal := (signal - mean(signal)) / sd(signal), .(yyyymm, var)]
  tmp <- dcast(tmp, yyyymm + permno ~ var, value.var = "signal")
  vv <- setdiff(names(tmp), c("yyyymm", "permno")) # variable names
  data <- merge(data, tmp, by = c("yyyymm", "permno"))
  data[is.na(data)] <- 0

  # --- univariate regressions

  # # regression weights
  # data[, w := 1 / .N, .(yyyymm)]

  out <- data.table()
  for (this_v in vv) {
    # this_v <- vv[1]
    # print(this_v)
    ff <- paste0("ret1 ~ ", this_v, " | yyyymm")
    ols <- feols(
      as.formula(ff), data
      # , weights = ~w
    )
    out <- rbind(out, data.table(
      spec_var = this_v, var = names(coef(ols)),
      coef = coef(ols), se = sqrt(diag(vcov(ols))),
      obs = nrow(data), ar2 = r2(ols)["ar2"], war2 = r2(ols)["war2"]
    ))
  }

  # mark the order in which variables are introduced
  tt <- readRDS(paste0("tmp/processed_signals/", stock_base, "/signal_availability.RDS"))
  tt <- tt[, .(first_ym = min(yyyymm)), var]
  tt <- tt[order(first_ym)]
  out <- merge(out, tt, by = "var")
  rm(tt)

  to_file <- paste0("tmp/processed_signals/", stock_base, "/signal_combining_panel/regression_result_one_by_one.RDS")
  dir.create(dirname(to_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(out, to_file)
  rm(out, this_v, ols, ff)


  # --- multivariate regressions
  ff <- paste0("ret1 ~ ", paste0(vv, collapse = "+"), " | yyyymm")
  ols <- feols(
    as.formula(ff), data
    # , weights = ~w
  )
  out <- data.table(
    var = names(coef(ols)), coef = coef(ols), se = sqrt(diag(vcov(ols))),
    obs = nrow(data), ar2 = r2(ols)["ar2"], war2 = r2(ols)["war2"]
  )
  to_file <- paste0("tmp/processed_signals/", stock_base, "/signal_combining_panel/regression_result.RDS")
  dir.create(dirname(to_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(out, to_file)

  # take out averages by period (doesn't matter for sorting)
  data <- data[, .(yyyymm, permno, regression_combined_ret = predict(ols))]
  data <- merge(data, data[, .(m = mean(regression_combined_ret)), yyyymm], by = "yyyymm")
  data[, regression_combined_ret := regression_combined_ret - m]
  data[, m := NULL]
  rm(ols, out, ff)

  to_file <- paste0("tmp/processed_signals/", stock_base, "/signal_combining_panel/combined_using_regression.RDS")
  dir.create(dirname(to_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(data, to_file)

  toc()
}

# # --- SANITY CHECK: take a look at results

# # don't always work
# readRDS("tmp/processed_signals/large/signal_combining_panel/regression_result.RDS")[, .(var, coef, tstat = coef / se)]
# readRDS("tmp/processed_signals/large/signal_combining_panel_ew/regression_result.RDS")[, .(var, coef, tstat = coef / se)]

# # this is what happens if we use the full-sample
# out <- readRDS("tmp/processed_signals/all/signal_combining_panel/regression_result.RDS")
# out[, .(var, coef, tstat = coef / se)]

# # -- correlation with simple combined signal?
# data <- readRDS("tmp/processed_signals/large/signal_combining_panel/combined_using_regression.RDS")
# names(data)[3] <- "reg_large"

# tmp <- readRDS("tmp/processed_signals/all/signal_combining_panel/combined_using_regression.RDS")
# names(tmp)[3] <- "reg_all"
# data <- data[tmp, on = c("yyyymm", "permno")]

# tmp <- readRDS("tmp/processed_signals/large/combined_signal.RDS")[, .(yyyymm, permno, avg_large = signal)]
# data <- merge(data, tmp, by = c("yyyymm", "permno"), all = T)

# tmp <- readRDS("tmp/processed_signals/all/combined_signal.RDS")[, .(yyyymm, permno, avg_all = signal)]
# data <- merge(data, tmp, by = c("yyyymm", "permno"), all = T)

# # let's compute average cross-sectional rank correlations - large and large have 90% rank correlation

# # 1. Cross-sectional Spearman correlations for each month
# vars <- c("reg_large", "reg_all", "avg_large", "avg_all")
# cs_corr <- data[,
#   {
#     C <- cor(.SD, method = "spearman", use = "pairwise.complete.obs")
#     # convert matrix to long format
#     as.data.table(as.table(C))
#   },
#   by = yyyymm,
#   .SDcols = vars
# ]

# # rename for clarity
# setnames(cs_corr, c("V1", "V2", "N"), c("var1", "var2", "rho"))

# # 2. Time-series average of correlations
# ts_avg <- cs_corr[, .(avg_rho = mean(rho, na.rm = TRUE)), by = .(var1, var2)]
# ts_avg[, avg_rho_round := round(avg_rho, 3)]
# dcast(ts_avg, var1 ~ var2, value.var = "avg_rho_round")

# # variation
# data[, sd(reg_large, na.rm = T), yyyymm][, mean(V1)]
# data[, sd(reg_all, na.rm = T), yyyymm][, mean(V1)]
# data[, sd(avg_large, na.rm = T), yyyymm][, mean(V1)]
# data[, sd(avg_all, na.rm = T), yyyymm][, mean(V1)]

# # check their return predictive power? Yes, reg slightly higher, but not by much
# tmp <- readRDS("../../data/Stocks/Monthly_CRSP.RDS")[, .(yyyymm, permno, ret)]
# tt <- unique(tmp[, .(yyyymm)]) %>%
#   mutate(mm = yyyymm %% 100) %>%
#   mutate(yyyymm_prev = fifelse(mm == 1, yyyymm - 100 + 11, yyyymm - 1)) %>%
#   select(-mm)
# tmp <- merge(tmp, tt, by = "yyyymm") %>%
#   mutate(yyyymm = yyyymm_prev) %>%
#   select(-yyyymm_prev)
# data <- merge(data, tmp, by = c("yyyymm", "permno"))
# rm(tmp)

# # average XS correlation w/ returns
# vars <- c("reg_large", "reg_all", "avg_large", "avg_all", "ret")
# cs_corr <- data[,
#   {
#     C <- cor(.SD, use = "pairwise.complete.obs")
#     # convert matrix to long format
#     as.data.table(as.table(C))
#   },
#   by = yyyymm,
#   .SDcols = vars
# ]

# # rename for clarity
# setnames(cs_corr, c("V1", "V2", "N"), c("var1", "var2", "rho"))

# # 2. Time-series average of correlations
# ts_avg <- cs_corr[, .(avg_rho = mean(rho, na.rm = TRUE)), by = .(var1, var2)]
# ts_avg[, avg_rho_round := round(avg_rho, 3)]
# dcast(ts_avg, var1 ~ var2, value.var = "avg_rho_round")

# feols(ret ~ reg_large | yyyymm, data, cluster = ~yyyymm)
# feols(ret ~ reg_all | yyyymm, data, cluster = ~yyyymm)
# feols(ret ~ avg_large | yyyymm, data, cluster = ~yyyymm)
# feols(ret ~ avg_all | yyyymm, data, cluster = ~yyyymm)
