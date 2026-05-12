library(this.path)
setwd(this.path::this.dir())

# Set the file path based on the operating system
os_type <- Sys.info()["sysname"]
if (os_type == "Windows") {
  # For Windows
  source("../runmefirst.R")
} else {
  # For macOS and Linux
  source("~/.runmefirst")
}

stock_base <- "large"
from_dir <- paste0("tmp/portfolio_results/", stock_base, "/statistics/cf_dr/")

# 1. Load all sensitivity files (h1, h2, h4, h8, h20, h40)
fill_horizons <- c(1, 2, 4, 8, 20, 40)
df_list <- lapply(fill_horizons, function(h) {
  file_path <- paste0(from_dir, "scale_h", h, ".RDS")
  if (file.exists(file_path)) {
    dt <- readRDS(file_path)
    return(dt)
  }
  return(NULL)
})

df_plot_all <- rbindlist(df_list, fill = TRUE) %>%
  mutate(
    hor_years = hor / 12,
    # Create a clean label for the facets
    fill_label = paste0("Fill: ", fill_h, " Quarters")
  ) %>%
  mutate(
    coef = ifelse(hor < 0, -coef, coef),
    se_plot = se
  ) %>% # Standard error stays positive
  # Ensure the factor model names are clean for the legend
  mutate(factor_model = replace(factor_model, factor_model == "sum_news", "cf+dr")) %>%
  setDT()

# ============================================================================
# PLOTTING FUNCTION WITH RIBBONS AND FACETS
# ============================================================================
plot_reversals_faceted <- function(data, title_text) {
  ggplot(data, aes(x = hor_years, y = coef, color = factor_model, fill = factor_model, group = factor_model)) +
    # Reference lines
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    geom_vline(xintercept = 0, linetype = "dotted", color = "red", alpha = 0.8) +

    # 95% Confidence Interval Band (1.96 * SE)
    geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se),
      color = NA, alpha = 0.15
    ) +

    # Main Estimate Line
    geom_line(lwd = 1) +

    # Facetting by Fill Forward Length
    facet_wrap(~fill_label, scales = "free_y") +

    # Formatting
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
    scale_x_continuous(breaks = seq(-5, 10, by = 2)) +
    labs(
      title = title_text,
      subtitle = "95% Newey-West Confidence Intervals",
      x = "Years from portfolio formation (T=0)",
      y = "Cumulative return",
      color = "Decomposition",
      fill = "Decomposition"
    ) +
    theme_classic() +
    theme(
      legend.position = "bottom",
      strip.background = element_blank(),
      strip.text = element_text(face = "bold"),
      plot.title = element_text(face = "bold")
    )
}

# ============================================================================
# GENERATE FIGURES
# ============================================================================

# 1. Symmetric Component: Combined Signal (All Fills)
p1 <- plot_reversals_faceted(
  df_plot_all[var_type == "sym" & var == "combined"],
  "Symmetric Component: Combined Signal (Sensitivity to Fill Forward)"
)
print(p1)


# Asymmetric
p2 <- plot_reversals_faceted(
  df_plot_all[var_type == "asy" & var == "combined"],
  "Asymmetric Component: Combined Signal (Sensitivity to Fill Forward)"
)
print(p2)


# # ---- JL: what to plot in the paper

# data <- df_plot_all[grepl("combined", var) & fill_h == 40]
# ggplot(data, aes(x = hor_years, y = coef, fill = var_type)) +
#   geom_line(aes(color = var_type)) +
#   geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = 0.15) +
#   theme_classic() +
#   scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
#   scale_x_continuous(breaks = seq(-5, 10, by = 2)) +
#   labs(
#     title = "Combined Signal (Sensitivity to Fill Forward)",
#     subtitle = "95% Newey-West Confidence Intervals",
#     x = "Years from portfolio formation (T=0)",
#     y = "Cumulative return",
#   ) +
#   geom_hline(yintercept = 0, linetype = "dashed", color = "gray50", lwd = 2) +
#   geom_vline(xintercept = 0, linetype = "dotted", color = "red", alpha = 0.8, lwd = 2) +
#   theme(text = element_text(size = 30))

# # how much did return go up in the return-based specification?
# tt <- readRDS("tmp/portfolio_results/large/statistics/newey_west/scale_by_total.RDS")
# tt <- tt[var == "combined" & from_hor == 1 & to_hor <= 120]
# ggplot(tt, aes(x = to_hor/12, y = coef, fill = var_type)) +
#   geom_line(aes(color = var_type)) +
#   geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = 0.15) +
#   theme_classic() +
#   scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
#   scale_x_continuous(breaks = seq(-5, 10, by = 2)) +
#   labs(
#     title = "Combined Signal (Sensitivity to Fill Forward)",
#     subtitle = "95% Newey-West Confidence Intervals",
#     x = "Years from portfolio formation (T=0)",
#     y = "Cumulative return",
#   ) + theme(text = element_text(size = 30))
