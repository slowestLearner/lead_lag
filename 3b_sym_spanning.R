# -- project CSM onto FM. Study spanning
library(this.path)
setwd(this.path::this.dir())
library(sandwich)
source("runmefirst.R")

# loop through two versions of factor mom
for (stock_base in c("all", "large")) {
  # stock_base <- 'large'

  print(paste("----- Processing stock_base:", stock_base, "-----"))
  time_start <- Sys.time()

  if (.Platform$OS.type == "windows") {
    base_dir <- "D:/Dropbox/Leadlag/formal_tests/code_final/tmp/factor_mom"
  } else {
    base_dir <- "~/Dropbox/SpeculativeIdeas/Leadlag/formal_tests/code_final/tmp/factor_mom"
  }
  full_path <- file.path(base_dir, stock_base, "factormom_ret.RDS")

  # get fm
  fm_data <- readRDS(full_path) %>%
    filter(var %in% c("fm")) %>%
    dplyr::select(yyyymm, hor, cumret_fm = cumret)

  # csm
  csm_data <- csm_data <- readRDS(paste0(
    "tmp/portfolio_results/", stock_base,
    "/scale_by_total/returns.RDS"
    # "/scale_by_total/remove_20pct_stk_FALSE/returns.RDS" NOTE: edited
  )) %>%
    dplyr::select(yyyymm, var, var_type, hor, ret = ret_fut)

  # get the combined version, with two subsamples
  csm_data_combined <- csm_data %>% filter(var == "combined")

  csm_data_combined_first <- csm_data_combined %>%
    filter(yyyymm <= 196212) %>%
    mutate(var = "combined_1926_1962")

  csm_data_combined_second <- csm_data_combined %>%
    filter(yyyymm > 196212) %>%
    mutate(var = "combined_1963_2023")

  csm_data <- rbindlist(list(csm_data, csm_data_combined_first, csm_data_combined_second), use.names = TRUE)
  rm(csm_data_combined, csm_data_combined_first, csm_data_combined_second)

  # compute cumulative returns for csm
  csm_data <- csm_data %>%
    arrange(yyyymm, var, var_type, hor) %>%
    group_by(yyyymm, var, var_type) %>%
    mutate(cumret = cumprod(1 + ret) - 1) %>%
    ungroup() %>%
    dplyr::select(-ret) %>%
    setDT()

  # --- spanning regressions by horizon
  data <- merge(csm_data, fm_data, by = c("yyyymm", "hor")) %>% na.omit()

  # helper function to estimate one spanning regression
  p.get_one <- function(this_data) {
    out <- data.table()
    this_hor <- this_data[1, hor]

    mm <- lm(cumret ~ 1, this_data)
    out <- rbind(out, data.table(
      spec_type = 1, spec_lab = "no control", alpha = mm$coef[1],
      beta = NA,
      se_alpha = sqrt(NeweyWest(mm, lag = this_hor)[1, 1]),
      se_beta = NA
    ))
    mm <- lm(cumret ~ cumret_fm, this_data)
    out <- rbind(out, data.table(
      spec_type = 2, spec_lab = "control for fm",
      alpha = mm$coef[1],
      beta = mm$coef[2],
      se_alpha = sqrt(NeweyWest(mm, lag = this_hor)[1, 1]),
      se_beta = sqrt(NeweyWest(mm, lag = this_hor)[2, 2])
    ))
    out[, hor := this_hor]
    out[, var := this_data[1, var]]
    out[, var_type := this_data[1, var_type]]
    return(out)
  }

  # estimate in parallel. Takes around a min with 6 cores
  data_list <- split(data, by = c("var", "var_type", "hor"))

  process <- data.table(idx = 1:length(data_list))
  block_size <- 300
  process[, block_idx := ceiling(idx / block_size)]

  out <- data.table()
  for (this_block in unique(process[, block_idx])) {
    tic(paste0("Processing block ", this_block, " of ", max(process[, block_idx])))
    chunk_indices <- process[block_idx == this_block, idx]
    chunk_data <- data_list[chunk_indices]

    # Run Parallel
    # future.packages = "sandwich" is REQUIRED for NeweyWest to work on Windows
    plan(multisession, workers = nc)
    block_res_list <- future_lapply(
      chunk_data,
      p.get_one,
      future.packages = c("sandwich", "data.table")
    )
    plan(sequential)
    out <- bind_rows(out, bind_rows(block_res_list))
    # out <- rbind(out, rbindlist(mclapply(data_list[process[block_idx == this_block, idx]], p.get_one, mc.cores = nc)))
    toc()
  }

  # save
  to_dir <- paste0("tmp/sym/", stock_base, "/spanning/cumulative_returns/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  saveRDS(out, paste0(to_dir, "projected_on_fm.RDS"))
  gc()

  print(paste("----- Overall time taken:", round(difftime(Sys.time(), time_start, units = "secs"), 2), "seconds -----"))
}
