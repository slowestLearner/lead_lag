# --- plot survival rate stuff
library(sandwich)
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

for (stock_base in c("all", "large")){ 
  
  from_dir <- paste0("../tmp/portfolio_results/", stock_base, 
                     "/just_total_with_fm_controls/remove_20pct_stk_FALSE/")
  
  to_dir <- paste0("../figs/forecasting_return/", stock_base,
                   "/reversals/remove_20pct_stk_FALSE/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  numobs <- readRDS(paste0(from_dir, "numobs.RDS"))
  
  numobs_summary <- numobs %>%
    dplyr::filter(hor == 0) %>%
    dplyr::select(-hor) %>%
    dplyr::left_join(numobs,
      by = c("yyyymm")
    ) %>%
    transmute(yyyymm, hor,
      total_frac = sum_n.y / sum_n.x,
      pos_frac = sum_n_pos.y / sum_n_pos.x,
      neg_frac = sum_n_neg.y / sum_n_neg.x
    ) %>%
    dplyr::arrange(hor, yyyymm) %>%
    dplyr::group_by(hor) %>%
    dplyr::summarise(across(everything(), ~ mean(.x, na.rm = TRUE))) %>%
    dplyr::ungroup() %>%
    dplyr::select(-yyyymm)
  
  
  
  
  
  # numbers for the paper
  data <- numobs_summary %>%
    pivot_longer(!hor, names_to = "bin", values_to = "fraction") %>%
    dplyr::mutate(bin = case_when(
      bin == "total_frac" ~ "Total",
      bin == "pos_frac" ~ "Positive",
      bin == "neg_frac" ~ "Negative"
    )) %>%
    dplyr::mutate(hor = hor / 12)
  
  pp <- ggplot(data, aes(x = hor, y = fraction, fill = bin)) +
    geom_line(aes(color = bin, lty = bin), lwd = 1) +
    theme_classic() +
    theme(legend.position = c(.2, .2), legend.title = element_blank(), text = element_text(size = 30)) +
    coord_cartesian(ylim = c(0, 1)) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    geom_hline(yintercept = 1, lty = 3) +
    labs(x = "Years after portfolio formation", y = "Survived") +
    scale_x_continuous(breaks = c(0:10))
  
  ggsave(paste0(to_dir, "survival_all_pos_neg.png"), pp, "png", w = 5, h = 4.5, units = "in", dpi = 300)
}