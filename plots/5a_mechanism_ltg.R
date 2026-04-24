# plot LTG
source("~/.runmefirst")
library(this.path)
setwd(this.path::this.dir())


# already computed
data_all <- readRDS("../../../tests/27_sym_mechanism_tests/tmp/analyst/dltg/cumulative.RDS") %>%
  filter(var_type == "sym") %>%
  select(-var_type) %>%
  group_by(var, hor) %>%
  summarize(coef = mean(dltg), se = sd(dltg) / sqrt(length(yyyymm)), .groups = "drop") %>%
  ungroup() %>%
  mutate(hor = hor / 4) %>%
  setDT()

# -- plot combined
data <- data_all[var == "combined"]

pp <- ggplot(data, aes(x = hor, y = coef)) +
  geom_line(lwd = 1) +
  geom_ribbon(aes(ymin = coef - 2 * se, ymax = coef + 2 * se), alpha = .2) +
  theme_classic() +
  scale_x_continuous(breaks = 0:10) +
  scale_y_continuous(labels = scales::percent_format(0.1)) +
  labs(x = "Years after portfolio formation", y = "Cumulative LTG revision") +
  theme(text = element_text(size = 25)) +
  geom_hline(yintercept = 0, lty = 2)

to_dir <- "../figs/understanding/sym/analyst/ltg/"
dir.create(to_dir, showWarnings = F, recursive = T)
ggsave(paste0(to_dir, "0_combined.png"), pp, "png", width = 4.5, height = 4)

# -- plot the others
data <- data_all[var != "combined"]

ggplot(data, aes(x = hor, y = coef, fill = var)) +
  geom_line(lwd = 1, aes(color = var)) +
  geom_ribbon(aes(ymin = coef - 2 * se, ymax = coef + 2 * se), alpha = .2) +
  theme_classic() +
  scale_x_continuous(breaks = 0:10) +
  scale_y_continuous(labels = scales::percent_format(0.1)) +
  labs(x = "Years after portfolio formation", y = "Cumulative LTG revision") +
  theme(text = element_text(size = 25)) +
  geom_hline(yintercept = 0, lty = 2)

# to_dir <- '../figs/understanding/sym/analyst/ltg/'
# dir.create(to_dir, showWarnings = F, recursive = T)
# ggsave(paste0(to_dir, "0_combined.png"), pp, "png", width = 4.5, height = 4)
