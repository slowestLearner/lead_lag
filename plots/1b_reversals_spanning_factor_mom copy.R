# Plot cumulative L/S returns with FM, and also FM-residualized signals
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

for (stock_base in c("all", "large")){  
  # -- just plot combined
  data <- readRDS(paste0("../tmp/sym/", stock_base,
                         "/spanning/cumulative_returns/projected_on_fm.RDS")) %>%
    filter(grepl("combined", var)) %>%
    select(spec_type, var, var_type, hor, coef = alpha, se = se_alpha) %>%
    mutate(spec_lab = ifelse(spec_type == 1, "Unadjusted", "FM-Residualized"))
  
  # also get hor = 0
  data_hor0 <- data %>%
    filter(hor == 1) %>%
    mutate(hor = 0, coef = 0, se = 0)
  data <- rbind(data, data_hor0) %>%
    mutate(hor = hor / 12) %>%
    filter(hor <= 10)
  rm(data_hor0)
  
  
  yy <- c(-.045, .04)
  this_var_type <- "total"
  this_var_type <- "sym"
  this_var_type <- "asy"
  
  to_dir <- paste0("../figs/sym/", stock_base, "/cumulative_returns_after_fm/combined/")
  dir.create(to_dir, showWarnings = F, recursive = T)
  
  vars <- unique(data[, var])
  var_types <- unique(data[, var_type])
  
  for (this_var in vars) {
    tic(this_var)
    yy <- c(min(data[var == this_var, coef - 2 * se]), max(data[var == this_var, coef + 2 * se]))
    for (this_var_type in var_types) {
      pp <- ggplot(data[(var == this_var) & (var_type == this_var_type)], aes(x = hor, y = coef, fill = reorder(spec_lab, spec_type))) +
        geom_line(aes(color = reorder(spec_lab, spec_type)), lwd = 1) +
        geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
        theme_classic() +
        scale_x_continuous(breaks = 0:10) +
        geom_hline(yintercept = 0, lty = 3) +
        scale_y_continuous(labels = scales::percent_format(1)) +
        labs(x = "Years after portfolio formation", y = "Cumulative return") +
        theme(legend.position = c(.25, .2), legend.title = element_blank()) +
        coord_cartesian(ylim = yy) +
        theme(text = element_text(size = 35))
      ggsave(paste0(to_dir, this_var, "_", this_var_type, ".png"), pp, "png", w = 4.5, h = 4.5)
    }
    toc()
  }
}