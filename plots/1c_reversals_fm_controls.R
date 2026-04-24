# show that FM and/or Burt-Hrdlicka does not get rid of the reversals
library(sandwich)
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

for (stock_base in c("all", "large")){  
  # original
  data <- readRDS(paste0("../tmp/portfolio_results/", stock_base,
                         "/statistics/newey_west/newey_west_fm_residuals.RDS")) %>% 
    filter(var_type %in% c("signal", "signal_FF3", "signal_Super_set", "signal_bh" ))
  
  # JD
  # tmp <- readRDS("../../code_202512_comparing_method/tmp/portfolio_results/statistics/newey_west/newey_west_fm_residuals.RDS")
  # data <- rbind(data, tmp)
  # rm(tmp)
  
  # merge with asy
  tmp <- readRDS(paste0("../tmp/portfolio_results/", stock_base,
                        "/statistics/newey_west/remove_20pct_stk_FALSE.RDS")) %>%
    filter(var_type == "asy" & to_hor <= 120) %>%
    filter(var %in% data[, var] & from_hor == 1) %>%
    select(var, var_type, hor = to_hor, coef, se)
  tmp_hor_0 <- tmp %>%
    filter(hor == 1) %>%
    mutate(hor = 0, coef = 0, se = 0)
  tmp <- rbind(tmp, tmp_hor_0) %>% mutate(hor = hor / 12)
  rm(tmp_hor_0)
  data <- rbind(data, tmp)
  rm(tmp)
  
  # rank specification orders
  tmp <- data.table(
    var_type = c("signal", "signal_FF3", "signal_Super_set", "signal_bh", "asy"),
    var_type_idx = 1:5,
    var_type_lab = c("No control", "FF3 control", "More controls", "Burt-Hrdlicka control", "Asymmetric")
  )
  data <- merge(data, tmp, by = "var_type")
  rm(tmp)
  
  # rank variables
  tmp <- data %>%
    select(var) %>%
    unique() %>%
    arrange(var) %>%
    mutate(var_idx = c(9, 3, 4, 1, 8, 6, 2, 7, 5) - 1) %>%
    arrange(var_idx) %>%
    setDT()
  data <- merge(data, tmp, by = "var")
  rm(tmp)
  data_all <- copy(data)
  
  
  # -- plot FM controls
  to_dir <- paste0("../figs/other_methods/", stock_base, "/fm/reversal/")
  dir.create(to_dir, showWarnings = F, recursive = T)
  
  for (this_idx in sort(unique(data_all[, var_idx]))) {
    print(this_idx)
    data <- copy(data_all[var_idx == this_idx])
    this_var <- data[1, var]
  
    yy <- c(data[, min(coef - 2 * se)], data[, max(coef + 2 * se)])
    if (yy[1] > -.04) {
      yy[1] <- -.04
    }
  
    pp <- ggplot(data[var_type_idx != 4], aes(x = hor, y = coef, fill = reorder(var_type_lab, var_type_idx))) +
      geom_line(aes(color = reorder(var_type_lab, var_type_idx)), lwd = 1) +
      geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
      theme_classic() +
      scale_x_continuous(breaks = 0:10) +
      geom_hline(yintercept = 0, lty = 3) +
      scale_y_continuous(labels = scales::percent_format(1)) +
      labs(x = "Years after portfolio formation", y = "Cumulative return") +
      theme(legend.position = c(.25, .17), legend.title = element_blank()) +
      theme(text = element_text(size = 35)) +
      coord_cartesian(ylim = yy)
    ggsave(paste0(to_dir, this_idx, "_", this_var, ".png"), pp, "png", w = 4.5, h = 4)
  }
  
  
  # -- plot FM controls
  to_dir <- paste0("../figs/other_methods/", stock_base, "/burt_hrdlicka/reversal/")
  dir.create(to_dir, showWarnings = F, recursive = T)
  
  for (this_idx in sort(unique(data_all[, var_idx]))) {
    print(this_idx)
    data <- copy(data_all[var_idx == this_idx])
    this_var <- data[1, var]
  
    yy <- c(data[, min(coef - 2 * se)], data[, max(coef + 2 * se)])
    if (yy[1] > -.04) {
      yy[1] <- -.04
    }
  
    pp <- ggplot(data[var_type_idx %in% c(1, 4, 5)], aes(x = hor, y = coef, fill = reorder(var_type_lab, var_type_idx))) +
      geom_line(aes(color = reorder(var_type_lab, var_type_idx)), lwd = 1) +
      geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
      theme_classic() +
      scale_x_continuous(breaks = 0:10) +
      geom_hline(yintercept = 0, lty = 3) +
      scale_y_continuous(labels = scales::percent_format(1)) +
      labs(x = "Years after portfolio formation", y = "Cumulative return") +
      theme(legend.position = c(.25, .17), legend.title = element_blank()) +
      theme(text = element_text(size = 35)) +
      coord_cartesian(ylim = yy)
    ggsave(paste0(to_dir, this_idx, "_", this_var, ".png"), pp, "png", w = 4.5, h = 4)
  }
}