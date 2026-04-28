# --- plot survival rate stuff
library(sandwich)
library(tidyr)
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")
options(width = 200)

for (stock_base in c("all", "large")) {
  # stock_base <- 'large'
  print(paste0("stock_base: ", stock_base))
  tic()

  from_dir <- paste0(
    "../tmp/portfolio_results/", stock_base,
    "/just_total_with_fm_controls/"
  )

  to_dir <- paste0(
    "../figs/forecasting_return/survival_rate/", stock_base,
    "/"
  )
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

  # get fraction of survival
  data <- readRDS(paste0(from_dir, "numobs.RDS")) %>% select(-sum_n)
  data <- melt(data, id.vars = c("yyyymm", "hor", "var"), variable.name = "type", value.name = "obs") %>%
    mutate(type = as.character(type)) %>%
    setDT()
  data <- merge(data, data[hor == 0, .(yyyymm, type, var, obs0 = obs)], by = c("yyyymm", "type", "var")) %>%
    filter(obs0 > 0) %>%
    setDT()
  data[, fraction := obs / obs0][, c("obs", "obs0") := NULL]
  data <- data[, .(fraction = mean(fraction)), .(hor, type, var)]

  # get ordering of variables
  tmp <- readRDS("../tmp/processed_signals/large/signal_availability.RDS")[, .(first_ym = min(yyyymm)), var]
  tmp <- rbind(data.table(var = "combined", first_ym = 1), tmp)[order(first_ym)] %>%
    mutate(var_idx = row_number()) %>%
    select(-first_ym) %>%
    setDT()
  data <- merge(data, tmp, by = "var")

  # mark the labels
  # tmp <- unique(data[, .(type)])[order(type)]
  # tmp[, lab := c(
  #   "Short leg, all", "Short leg, 10%", "Short leg, 20%", "Short leg, 50%",
  #   "Long leg, all", "Long leg, 10%", "Long leg, 20%", "Long leg, 50%"
  # )]
  # tmp[, idx := 1:nrow(tmp)]

  # 1. Prepare the data with the original labels
  tmp <- unique(data[, .(type)])[order(type)]
  labels_vec <- c(
    "Short leg, all", "Short leg, 10%", "Short leg, 20%", "Short leg, 50%",
    "Long leg, all", "Long leg, 10%", "Long leg, 20%", "Long leg, 50%"
  )
  tmp[, lab := labels_vec]
  data <- merge(data, tmp, by = "type")

  # 2. Define the manual "matching" colors (using a paired palette)
  # We use the same color twice: once for Short, once for Long
  my_colors <- c(
    "Short leg, all" = "#999999", "Long leg, all" = "#999999", # Gray
    "Short leg, 10%" = "#E41A1C", "Long leg, 10%" = "#E41A1C", # Red
    "Short leg, 20%" = "#377EB8", "Long leg, 20%" = "#377EB8", # Blue
    "Short leg, 50%" = "#4DAF4A", "Long leg, 50%" = "#4DAF4A" # Green
  )

  # 3. Define the linetypes (1 = solid, 2 = dashed)
  my_linetypes <- c(
    "Short leg, all" = 2, "Long leg, all" = 1,
    "Short leg, 10%" = 2, "Long leg, 10%" = 1,
    "Short leg, 20%" = 2, "Long leg, 20%" = 1,
    "Short leg, 50%" = 2, "Long leg, 50%" = 1
  )

  # # Define the mapping more explicitly
  # tmp <- unique(data[, .(type)])[order(type)]
  # tmp[, `:=`(
  #   Leg = rep(c("Short", "Long"), each = 4),
  #   Group = rep(c("All", "10%", "20%", "50%"), times = 2)
  # )]

  # # Create the label for the legend if you still want a single legend entry,
  # # but it's cleaner to map them separately.
  # data <- merge(data, tmp, by = "type")
  rm(tmp)

  # scale for plotting
  # yy <- c(min(data[, fraction]), max(data[, fraction]))
  yy <- c(.4, 1)

  for (this_idx in unique(data[, var_idx])) {
    # this_idx <- 1
    data_this <- data[var_idx == this_idx]

    # pp <- ggplot(data_this, aes(x = hor / 12, y = fraction, color = reorder(lab, idx))) +
    #   geom_line() +
    #   theme_classic() +
    #   theme(legend.position = c(.2, .3), legend.title = element_blank(), text = element_text(size = 30)) +
    #   coord_cartesian(ylim = yy) +
    #   scale_y_continuous(labels = scales::percent_format(1)) +
    #   geom_hline(yintercept = 1, lty = 3) +
    #   scale_x_continuous(breaks = 0:10) +
    #   labs(x = "Years after portfolio formation", y = "Survived")

    pp <- ggplot(data_this, aes(x = hor / 12, y = fraction, color = lab, linetype = lab)) +
      geom_line(linewidth = 0.5) +
      theme_classic() +
      # This merges the two legends into one because the labels match exactly
      scale_color_manual(values = my_colors) +
      scale_linetype_manual(values = my_linetypes) +
      theme(
        legend.position = c(.2, .32),
        legend.title = element_blank(),
        text = element_text(size = 35),
        legend.key.width = unit(1.5, "cm") # Make the lines in legend long enough to see dash
      ) +
      coord_cartesian(ylim = yy) +
      scale_y_continuous(labels = scales::percent_format(1)) +
      geom_hline(yintercept = 1, lty = 3) +
      scale_x_continuous(breaks = 0:10) +
      labs(x = "Years after portfolio formation", y = "Survived")

    ggsave(paste0(to_dir, this_idx, "_", data_this[1, var], ".png"), pp, "png", w = 5, h = 4.5, units = "in", dpi = 300)
  }
  toc()
}
