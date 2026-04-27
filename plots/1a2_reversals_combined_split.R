# --- Plot cumulative L/S returns for combined predictor but split into two time periods
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'

  from_dir <- paste0("../tmp/portfolio_results/", stock_base, "/statistics/newey_west/")

  # cumulative returns
  data_this <- readRDS(paste0(from_dir, "scale_by_total.RDS")) %>%
    filter(var %in% c("combined_1926_1962", "combined_1963_2023")) %>%
    filter(from_hor == 1) %>%
    filter(to_hor <= 120) %>%
    dplyr::rename(hor = to_hor) %>%
    select(-hor_idx, -from_hor) %>%
    mutate(label = ifelse(var == "combined_1926_1962", "1926-1962", "1963-2023"))

  to_dir_total <- paste0("../figs/forecasting_return/reversals/total/", stock_base, "/")
  # to_dir_sep <- paste0("../figs/forecasting_return/reversals/sep/", stock_base, "/")

  dir.create(to_dir_total, showWarnings = F, recursive = T)
  # dir.create(to_dir_sep, showWarnings = F, recursive = T)

  # add a zero
  data_zero <- data_this %>%
    filter(hor == 1) %>%
    mutate(hor = 0, coef = 0, se = 0)
  data_this <- rbind(data_this, data_zero) %>% mutate(hor = hor / 12)

  # plot total
  pp <- ggplot(data_this[var_type == "total"], aes(x = hor, y = coef, fill = label)) +
    geom_line(lwd = 1, aes(color = label)) +
    geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
    theme_classic() +
    scale_x_continuous(breaks = 0:10) +
    geom_hline(yintercept = 0, lty = 3) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(x = "Years after portfolio formation", y = "Cumulative return") +
    theme(text = element_text(size = 35), legend.position = c(.2, .2), legend.title = element_blank())
  ggsave(paste0(to_dir_total, "1_combined_split.png"), pp, "png", width = 4.5, height = 4)
}
