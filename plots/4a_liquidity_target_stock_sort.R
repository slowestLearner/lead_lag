# sorted into deciles
source("~/.runmefirst")
library(this.path)
library(corrplot)
library(RColorBrewer)
setwd(this.path::this.dir())

# already computed
data <- readRDS("../../../tests/27_sym_mechanism_tests/tmp/liquidity/deciles/require_equal_weight_for_total_signal/statistics_deciles_and_quintiles.RDS") %>%
  filter(sort_type == "decile" & liq_var == "minus_spread" & var_type != "total" & bin_liq != 11 & !grepl("combined", var)) %>%
  rename(coef = ret1)

# rank vars
tmp <- unique(data[, .(var)]) %>% mutate(var_idx = c(8, 2, 3, 7, 5, 1, 6, 4))
data <- merge(data, tmp, by = "var") %>%
  arrange(var_idx) %>%
  setDT()
rm(tmp)

# make expressive
data[, var_type := ifelse(var_type == "sym", "Symmetric", "Asymmetric")]

to_dir <- "../figs/understanding/liq_target_stock_sort/by_minus_spread/"
dir.create(to_dir, showWarnings = F, recursive = T)

for (this_idx in unique(data[, var_idx])) {
  # this_idx <- 1
  pp <- ggplot(data[var_idx == this_idx], aes(x = bin_liq, y = coef, fill = var_type)) +
    geom_point(aes(color = var_type), cex = 3) +
    geom_line(aes(color = var_type), lwd = 1) +
    geom_ribbon(aes(ymin = coef - 2 * se, ymax = coef + 2 * se), alpha = .2) +
    theme_classic() +
    geom_hline(yintercept = 0, lty = 2) +
    scale_x_continuous(breaks = 1:10) +
    labs(x = "Liquidity Decile", y = "Portfolio return (one month)") +
    scale_y_continuous(labels = scales::percent_format(0.01))
  if (this_idx == 1) {
    pp <- pp + theme(legend.position = c(.2, .8), legend.title = element_blank(), text = element_text(size = 25))
  } else {
    pp <- pp + theme(legend.position = c(1.2, .8), legend.title = element_blank(), text = element_text(size = 25))
  }


  to_file <- paste0(to_dir, this_idx, "_", data[var_idx == this_idx][1, var], ".png")
  ggsave(to_file, pp, "png", width = 4.5, height = 4)
}
