# Do FM controls get rid of the reversal?
source("~/.runmefirst")
library(sandwich)
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code

data <- readRDS("../tmp/portfolio_results/statistics/newey_west/newey_west_fm_residuals.RDS")

# rank specification orders
tmp <- unique(data[, .(var_type)]) %>%
  mutate(spec_idx = row_number()) %>%
  mutate(spec_lab = c("Original", "FF3 Residual", "FF3 + ind residual", "FF3 + ind + char residual"))
data <- merge(data, tmp, by = "var_type")
rm(tmp)

# rank variables
tmp <- data %>%
  select(var) %>%
  unique() %>%
  arrange(var) %>%
  mutate(var_idx = c(9, 3, 4, 1, 8, 6, 2, 7, 5))
data <- merge(data, tmp, by = "var")
rm(tmp)

to_dir <- "../figs/other_methods/fm/reversal/"
dir.create(to_dir, showWarnings = F, recursive = T)

for (this_idx in unique(data[, var_idx])) {
  # this_idx <- 1
  print(this_idx)
  subdata <- data %>% filter(var_idx == this_idx)
  pp <- ggplot(subdata, aes(x = hor, y = coef, fill = reorder(spec_lab, spec_idx))) +
    geom_line(aes(color = reorder(spec_lab, spec_idx)), lwd = 1) +
    geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
    theme_classic() +
    scale_x_continuous(breaks = 0:10) +
    geom_hline(yintercept = 0, lty = 3) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(x = "Years after portfolio formation", y = "Cumulative return") +
    theme(legend.position = c(.25, .17), legend.title = element_blank()) +
    theme(text = element_text(size = 35)) +
    coord_cartesian(ylim = c(-.05, NA))
  ggsave(paste0(to_dir, this_idx, "_", subdata[1, var], ".png"), pp, "png", w = 4.5, h = 4)
}
