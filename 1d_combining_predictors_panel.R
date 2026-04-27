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

  out <- data.table()
  for (this_v in vv) {
    # print(this_v)
    ff <- paste0("ret1 ~ ", this_v, " | yyyymm")
    ols <- feols(
      as.formula(ff), data,
      cluster = ~ yyyymm + permno
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
    as.formula(ff), data,
    cluster = ~ yyyymm + permno
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

# # -- report correlation w/ simple combined siangl
# data <- readRDS("tmp/processed_signals/large/signal_combining_panel/combined_using_regression.RDS")[, .(yyyymm, permno, reg = regression_combined_ret)]

# tmp <- readRDS("tmp/processed_signals/large/combined_signal.RDS")[, .(yyyymm, permno, avg = signal)]
# data <- merge(data, tmp, by = c("yyyymm", "permno"))
# rm(tmp)

# data[, cor(reg, avg), yyyymm][, mean(V1)]
# data[, cor(reg, avg, method = "spearman"), yyyymm][, mean(V1)]
