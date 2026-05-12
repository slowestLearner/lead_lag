library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")


stock_base <- "all"
from_dir <- paste0("tmp/portfolio_results/", stock_base, "/statistics/cf_dr/")




df_plot <- readRDS(file = paste0(from_dir, "scale_by_total.RDS")) %>%
  filter(from_hor == 1) %>%
  filter(to_hor <= 120) %>%
  dplyr::select(var, var_type, factor_model, hor = to_hor, coef, se) %>%
  mutate(hor = hor / 12) %>%
  dplyr::arrange(var, hor, var_type, factor_model) %>%
  setDT()


data_zero <- df_plot %>%
  filter(hor == 1) %>%
  mutate(hor = 0, coef = 0, se = 0)

df_plot_all <- bind_rows(data_zero, df_plot)

df_plot_i <- df_plot_all[var_type == "sym" & var == "combined"] %>%
  dplyr::select(-var, -var_type) %>%
  dplyr::mutate(factor_model = replace(factor_model, factor_model == "sum_news", "cf+dr")) %>%
  setDT()



ggplot(df_plot_i%>%
         dplyr::filter(hor <=10), 
       aes(x = hor, y = coef, color = factor_model, group = factor_model)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
  scale_y_continuous(labels = scales::percent_format(1)) +
  geom_line(lwd = 1) +
  labs(title = "Symmetric of Combined Signal",
       x = "Years after portfolio formation",
       y = "Cumulative return",
       color = "Decomposition") +
  scale_x_continuous(breaks = 0:10) +
  theme_classic() +
  theme(legend.position = "bottom")


df_plot_i <- df_plot_all[var_type == "asy" & var == "combined"] %>%
  dplyr::select(-var, -var_type) %>%
  dplyr::mutate(factor_model = replace(factor_model, factor_model == "sum_news", "cf+dr")) %>%
  setDT()



ggplot(df_plot_i, aes(x = hor, y = coef, color = factor_model, group = factor_model)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
  scale_y_continuous(labels = scales::percent_format(1)) +
  geom_line(lwd = 1) +
  labs(title = "Asymmetric of Combined Signal",
       x = "Years after portfolio formation",
       y = "Cumulative return",
       color = "Decomposition") +
  scale_x_continuous(breaks = 0:10) +
  theme_classic() +
  theme(legend.position = "bottom")
