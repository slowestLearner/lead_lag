# --- plot autocorrelations of signals
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")
options(width = 200)

# CSM signals
data <- readRDS("../tmp/processed_signals/large/signal_autocorrelation.RDS")[fe_type == "none", .(var, coef, se, type = "Cross-stock momentum")]

# rank the signals
tmp <- readRDS("../tmp/processed_signals/large/signal_availability.RDS")[, .(first_ym = min(yyyymm), var_lab = last(var_lab)), .(var)]
tmp <- rbind(data.table(var = "combined", first_ym = 1, var_lab = "Combined"), tmp)[order(first_ym)] %>%
  mutate(idx = row_number()) %>%
  select(-first_ym)

data <- merge(data, tmp, by = "var")[order(idx)]
rm(tmp)

# chars, choose some thta we are familiar with
tmp <- readRDS("../tmp/one_off/characteristics_autocorrelation.RDS")[fe_type == "none", .(var, coef, se, type = "Characteristics")]
tt <- data.table(
  var = c("size", "bm", "mom", "gp", "agr", "realized_vol", "rev"),
  var_lab = c("Size", "B/M", "Mom", "Prof", "Asset Gr", "Realized Vol", "1M Reversal"),
  idx = 10:16
)
tmp <- merge(tmp, tt, by = "var")[order(idx)]
data <- rbind(data, tmp)
rm(tmp, tt)

pp <- ggplot(data, aes(x = idx, y = coef)) +
  geom_bar(stat = "identity", position = "dodge", aes(fill = type)) +
  geom_errorbar(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se),
    position = "dodge", width = 0.5
  ) +
  scale_x_discrete(limits = factor(data[, unique(idx)]), labels = data[, var_lab]) +
  theme_classic() +
  scale_y_continuous(labels = scales::percent_format(1)) +
  labs(x = element_blank(), y = "Monthly autocorrelation") +
  theme(text = element_text(size = 35), legend.title = element_blank(), legend.position = c(.25, .8), axis.text.x = element_text(angle = 45, hjust = 1))


to_file <- "../figs/one_off/char_autocorrelation/autocorrelation.png"
dir.create(dirname(to_file), showWarnings = F, recursive = T)
ggsave(to_file, pp, "png", width = 5, height = 4)
