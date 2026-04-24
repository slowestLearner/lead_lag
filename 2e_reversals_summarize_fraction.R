# -- For horizons 1 and 12m, summarize the fraction of returns attributed to sym or asy
source("runmefirst.R")
library(this.path)
setwd(this.path::this.dir())

# point estimates
for (stock_base in c("all", "large")) {
  # stock_base <- "all"
  tic(stock_base)
  folder_path <- paste0("tmp/portfolio_results/", stock_base, "/statistics/")
  coef_data <- readRDS(paste0(folder_path, "newey_west/scale_by_total.RDS")) %>%
    dplyr::rename(hor = to_hor) %>%
    filter(hor_idx %in% 1:2) %>%
    select(var, var_type, hor, coef) %>%
    dcast(var + hor ~ var_type, value.var = "coef") %>%
    mutate(spec_idx = row_number()) %>%
    setDT()


  cov_data <- readRDS(paste0(folder_path, "newey_west_fraction/scale_by_total.RDS")) %>%
    filter(hor %in% c(1, 12)) %>%
    select(-weight_type, -factor_model) %>%
    merge(coef_data[, .(spec_idx, var, hor)], by = c("var", "hor"))

  # do ratio and newey-west for a specification
  p.get_one_spec <- function(this_spec_idx) {
    # this_spec_idx <- 1
    this_coef <- coef_data[spec_idx == this_spec_idx]

    this_cov <- cov_data[spec_idx == this_spec_idx] %>% arrange(col_name)
    cov <- this_cov %>%
      data.table::dcast(row_name ~ col_name, value.var = "cov") %>%
      arrange(row_name) %>%
      select(-row_name) %>%
      as.matrix()

    cov <- this_cov %>%
      data.table::dcast(row_name ~ col_name, value.var = "cov") %>%
      arrange(row_name) %>% # 1. Sort rows alphabetically
      select(-row_name) %>% # 2. Remove the ID column
      select(all_of(sort(names(.)))) %>% # 3. Sort remaining columns alphabetically
      as.matrix()

    f_a <- t(as.matrix(this_coef[, .(1 / total, 0, -asy / total^2)]))
    f_s <- t(as.matrix(this_coef[, .(0, 1 / total, -asy / total^2)]))

    out <- this_coef[, .(type = "asy_to_total", ratio = asy / total, se = sqrt((t(f_a) %*% cov %*% f_a)[1, 1]))]
    out <- rbind(out, this_coef[, .(type = "sym_to_total", ratio = sym / total, se = sqrt((t(f_s) %*% cov %*% f_s)[1, 1]))])
    out <- out %>% mutate(var = this_coef[1, var], hor = this_coef[1, hor])

    return(out)
  }
  plan(multisession, workers = nc)
  results_list <- future_lapply(
    unique(coef_data[, spec_idx]),
    p.get_one_spec,
    future.packages = c("dplyr", "data.table")
  )
  plan(sequential)
  out <- rbindlist(results_list)

  # out <- Reduce(rbind, mclapply(unique(coef_data[, spec_idx]), p.get_one_spec, mc.cores = nc))
  to_dir <- paste0(folder_path, "newey_west_fraction/summary/")
  dir.create(to_dir, recursive = TRUE, showWarnings = FALSE)
  saveRDS(out, paste0(to_dir, "scale_by_total.RDS"))
  toc()
}

# # --- SANITY - matched
# source("runmefirst.R")

# data_old <- readRDS("tmp/portfolio_results/all/statistics/newey_west_fraction/summary/port_returns_remove_20pct_stk_FALSE.RDS")
# data_new <- readRDS("tmp/portfolio_results/all/statistics/newey_west_fraction/summary/scale_by_total.RDS")
# data <- data_old[data_new, on = c("var", "hor", "type"), nomatch = 0]

# plot(data[, ratio], data[, i.ratio], pch = 16, cex = 3)
# abline(0,1, lwd = 2)

# plot(data[, se], data[, i.se], pch = 16, cex = 3)
# abline(0,1, lwd = 2)
