# -- compute correlation between CSM returns and FM (factor momentum)
# code adapted from JD/formal_tests/7_sym.Rmd
library(this.path)

# set working directory to be the root of the code
setwd(this.path::this.dir())
source("runmefirst.R")

for (stock_base in c("all", "large")) {
  tic(stock_base)
  if (.Platform$OS.type == "windows") {
    base_dir <- "D:/Dropbox/Leadlag/formal_tests/code_final/tmp/factor_mom"
  } else {
    base_dir <- "~/Dropbox/SpeculativeIdeas/Leadlag/formal_tests/code_final/tmp/factor_mom"
  }

  full_path <- file.path(base_dir, stock_base, "factormom_ret.RDS")

  # This is from the previous version
  mom_data <- readRDS(full_path) %>%
    filter(hor == 1) %>%
    filter(var == "fm") %>%
    dplyr::select(yyyymm, fm = ret)

  # csm
  csm_data <- readRDS(paste0(
    "tmp/portfolio_results/", stock_base,
    "/scale_by_total/returns.RDS"
    # "/scale_by_total/remove_20pct_stk_FALSE/returns.RDS" NOTE: edited
  )) %>%
    filter(hor == 1) %>%
    dplyr::select(yyyymm, var, var_type, ret = ret_fut)

  # also get the combined version, with two subsamples
  csm_data_combined <- csm_data %>% filter(var == "combined")

  csm_data_combined_first <- csm_data_combined %>%
    filter(yyyymm <= 196212) %>%
    mutate(var = "combined_1926_1962")
  csm_data_combined_second <- csm_data_combined %>%
    filter(yyyymm > 196212) %>%
    mutate(var = "combined_1963_2023")

  csm_data <- rbindlist(list(csm_data, csm_data_combined_first, csm_data_combined_second), use.names = TRUE)
  rm(csm_data_combined, csm_data_combined_first, csm_data_combined_second)

  # -- merge and get pairwise correlations
  data <- merge(csm_data, mom_data, by = "yyyymm")

  cor_data <- data[, .(obs = .N, cor_with_fm = cor(ret, fm)), .(var, var_type)]

  to_dir <- paste0("tmp/sym/", stock_base, "/summaries/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  saveRDS(cor_data, paste0(to_dir, "cor_with_fm.RDS"))
  toc()
}
