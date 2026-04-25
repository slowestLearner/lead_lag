# --  Runs time-series spanning regressions of CSM returns on FF factors to test whether alpha survives controls.
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

# CSM returns
for (stock_base in c("all", "large")) {
  tic(stock_base)

  # get CSM next-month returns
  file_path <- file.path("tmp/portfolio_results", stock_base, "scale_by_total")
  data <- readRDS(paste0(file_path, "/returns.RDS")) %>%
    filter((var_type == "total") & (hor == 1)) %>%
    select(yyyymm, var, ret_fut)

  # shift one period to align with factors
  data <- data %>%
    mutate(mm = yyyymm - 100 * floor(yyyymm / 100)) %>%
    mutate(yyyymm = ifelse(mm == 12, yyyymm + 100 - 11, yyyymm + 1)) %>%
    select(yyyymm, var, ret = ret_fut) %>%
    filter(yyyymm <= 202312)

  # # create the combined version
  # data <- rbind(data, data[, .(var = "combined", ret = mean(ret)), yyyymm])

  # merge with factors
  # run 5a_download_factors.R to download the factors from French-Library
  factor_path <- file.path("tmp/factors")
  factor_data <- readRDS(paste0(factor_path, "/ff_factors_1926_2023.Rds")) %>%
    dplyr::mutate(yyyymm = year(date) * 100 + month(date)) %>%
    dplyr::select(-date) %>%
    dplyr::rename(rev = strev)

  # merge. NOTE: require full data, so this starts from 1963
  data <- merge(data, factor_data, by = "yyyymm") %>%
    mutate(retRf = (1 + ret) / (1 + rf) - 1) %>%
    select(-rf, -ret) %>%
    na.omit() %>%
    setDT()
  rm(factor_data)

  # run some regressions
  spec_data <- data.table(
    formula = c(
      "retRf ~ 1",
      "retRf ~ mktRf",
      "retRf ~ mktRf + smb + hml",
      "retRf ~ mktRf + smb + hml + rmw + cma",
      "retRf ~ mktRf + smb + hml + rmw + cma + mom",
      "retRf ~ mktRf + smb + hml + rmw + cma + mom + rev"
    ),
    model_name = c("none", "capm", "ff3", "ff5", "ff5c", "ff5cr"),
    model_idx = 1:6
  )

  # helper function to run one spanning regression
  p.get_one_var <- function(this_var) {
    this_data <- data %>% filter(var == this_var)
    out <- data.table()
    for (this_idx in spec_data[, model_idx]) {
      mm <- lm(spec_data[model_idx == this_idx, formula], this_data)
      ss <- summary(mm)

      out <- rbind(out, data.table(
        model_idx = this_idx,
        reg_var = rownames(ss$coef),
        coef = ss$coef[, 1],
        se = ss$coef[, 2],
        n = nrow(mm$model),
        adjr2 = ss$adj.r.squared
      ))
    }
    out[, var := this_var]
    out <- merge(out, spec_data[, .(model_idx, model_name)], by = "model_idx")
    return(out)
  }

  vars <- unique(data[, var])
  out <- rbindlist(lapply(vars, p.get_one_var))

  to_dir <- paste0("tmp/other_methods/", stock_base, "/time_series_spanning/")
  dir.create(to_dir, showWarnings = F, recursive = T)
  saveRDS(out, paste0(to_dir, "csm_require_ff5.RDS"))
  toc()
}
