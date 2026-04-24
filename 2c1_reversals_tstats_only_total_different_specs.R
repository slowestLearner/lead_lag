# Compute Newey-West t-statistics for portfolio returns for various versions of the total signal
source("runmefirst.R")
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code

for (stock_base in c("all", "large")) {
  # stock_base <- 'all'
  file_base <- file.path("tmp/portfolio_results", stock_base, "just_total_with_fm_controls")

  data <- readRDS(paste0(file_base, "/returns.RDS")) %>%
    select(yyyymm, var, var_type, hor, ret = ret_fut)

  # compute cumulative returns by specification
  data <- data %>%
    arrange(yyyymm, var, var_type, hor) %>%
    group_by(yyyymm, var, var_type) %>%
    mutate(cumret = cumprod(1 + ret) - 1) %>%
    ungroup() %>%
    select(-ret) %>%
    setDT()

  # summarize
  data_list <- split(data, by = c("var", "var_type", "hor"))

  # helper function to compute Newey-West t-statistics for a specification
  p.get_one_nw <- function(this_data) {
    this_hor <- this_data[1, hor]
    mm <- lm(cumret ~ 1, this_data)
    return(data.table(
      var = this_data[1, var],
      var_type = this_data[1, var_type],
      hor = this_data[1, hor],
      coef = mm$coef[1],
      se = sqrt(NeweyWest(mm, lag = this_hor)[1, 1])
    ))
  }

  process <- data.table(idx = 1:length(data_list))
  block_size <- 300
  process[, block_idx := ceiling(idx / block_size)]

  # takes around one min
  data <- data.table()
  plan(multisession, workers = nc)
  for (this_block in unique(process[, block_idx])) {
    tic(paste0("Processing block ", this_block, " of ", max(process[, block_idx])))
    chunk_indices <- process[block_idx == this_block, idx]
    chunk_data <- data_list[chunk_indices]
    results_list <- future_lapply(chunk_data, p.get_one_nw, future.packages = "sandwich")

    data <- rbind(data, rbindlist(results_list))
    toc()
  }
  plan(sequential)

  # also get hor = 0 (for plotting purposes)
  data_hor0 <- data %>%
    filter(hor == 1) %>%
    mutate(hor = 0, coef = 0, se = 0)

  # turn hor into years
  data <- rbind(data, data_hor0) %>%
    mutate(hor = hor / 12) %>%
    filter(hor <= 10)
  rm(data_hor0)

  to_dir <- paste0("tmp/portfolio_results/", stock_base, "/statistics/newey_west/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

  saveRDS(data, paste0(to_dir, "newey_west_fm_residuals.RDS"))
}

# --- SANITY: check results

data <- readRDS("tmp/portfolio_results/all/statistics/newey_west/newey_west_fm_residuals.RDS")
data[, hor_m := round(hor * 12)]
dcast(data[(var == "combined") & (hor_m %in% c(1, 12, 60, 84, 120)), round(mean(100 * coef), 2), .(hor_m, var_type)], var_type ~ hor_m)

ggplot(data[var == "combined"], aes(x = hor, y = coef, fill = var_type)) +
  geom_line(aes(color = var_type), lwd = 2) +
  geom_hline(yintercept = 0, lty = 3, lwd = 2) +
  theme_classic()
