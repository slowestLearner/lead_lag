# --- Plot cumulative return of CSM and factor-mom portfolios (NOT REDONE)
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'

  # combined CSM
  data <- readRDS(paste0(
    "../tmp/portfolio_results/", stock_base,
    "/statistics/newey_west/scale_by_total.RDS"
  )) %>%
    filter(var == "combined") %>%
    filter(var_type == "sym") %>%
    filter(from_hor == 1) %>%
    filter(to_hor <= 120) %>%
    mutate(var = "Symmetric CSM") %>%
    select(hor = to_hor, var, coef, se)

  # FM
  fm_data <- readRDS(paste0(
    "../tmp/factor_mom/", stock_base,
    "/factormom_ret.RDS"
  )) %>%
    filter(var %in% c("fm")) %>%
    filter(hor <= 120) %>%
    select(yyyymm, hor, cumret_fm = cumret) %>%
    arrange(yyyymm, hor)

  # do Newey-west
  p.get_one_nw <- function(this_hor) {
    mm <- lm(cumret_fm ~ 1, fm_data[hor == this_hor])
    return(data.table(
      hor = this_hor,
      coef = mm$coef[1],
      se = sqrt(NeweyWest(mm, lag = this_hor)[1, 1])
    ))
  }

  fm_data <- rbindlist(lapply(unique(fm_data[, hor]), p.get_one_nw))
  gc()

  fm_data[, var := "Factor momentum"]
  data <- rbind(data, fm_data)
  rm(fm_data)

  # get zero
  data_zero <- data %>%
    filter(hor == 1) %>%
    mutate(hor = 0, coef = 0, se = 0)
  data <- rbind(data_zero, data)
  data[, hor := hor / 12]
  rm(data_zero)

  pp <- ggplot(data, aes(x = hor, y = coef, fill = var)) +
    geom_line(lwd = 1, aes(color = var)) +
    geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
    theme_classic() +
    scale_x_continuous(breaks = 0:10) +
    geom_hline(yintercept = 0, lty = 3) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(x = "Years after portfolio formation", y = "Cumulative return") +
    theme(text = element_text(size = 25), legend.position = c(.2, .15), legend.title = element_blank())


  to_dir <- paste0("../figs/sym/", stock_base, "/similarity_with_fm/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  ggsave(paste0(to_dir, "reversal_csm_sym_vs_fm.png"), pp, "png", width = 4.5, height = 4)
}
