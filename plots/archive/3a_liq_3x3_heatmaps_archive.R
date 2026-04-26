# Let's produce some heatmaps - perhaps just for the asy
source("~/.runmefirst")
library(this.path)
library(corrplot)
library(RColorBrewer)
setwd(this.path::this.dir())

# already computed
data <- readRDS("../tmp/portfolio_results/by_liquidity_3x3/by_minus_spread_demeaned_ret1/1m_ret_by_3x3_scaled.RDS") %>%
  group_by(var, var_type, source_liq_bin, target_liq_bin) %>%
  summarize(mean_ret1 = 100 * mean(ret1), se_ret1 = 100 * sd(ret1) / sqrt(length(yyyymm))) %>%
  ungroup() %>%
  setDT()

# order them
tmp <- unique(data[, .(var)]) %>%
  arrange(var) %>%
  setDT()
tmp[, var_idx := c(9, 3, 4, 1, 8, 6, 2, 7, 5)]
data <- merge(data, tmp, by = "var") %>%
  arrange(var_idx) %>%
  setDT()
rm(tmp)

# 1. Filter the data for the specific slice you want to plot
var_types <- unique(data[, var_type])

to_dir_base <- "../figs/understanding/liq_3x3/1m_return_map/"
dir.create(to_dir_base, showWarnings = F, recursive = T)

# Define your color palette once (Blue -> White -> Red)
col_palette <- colorRampPalette(c("forestgreen", "white", "royalblue"))(200)

for (this_var_type in var_types) {
  print(this_var_type)
  # this_var_type <- var_types[1]

  to_dir <- paste0(to_dir_base, this_var_type, "/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

  # Calculate global range for this variable type so colors are consistent
  yy <- range(data[var_type == this_var_type, mean_ret1], na.rm = TRUE)
  max_abs_yy <- max(abs(yy))
  limit_range <- c(-max_abs_yy, max_abs_yy)

  for (this_idx in unique(data[, var_idx])) {
    # this_idx <- 1


    # 1. Reshape Data to Matrix (Wide Format)
    plot_data <- data[(var_idx == this_idx) & (var_type == this_var_type)]

    # dcast: Rows = target (Lag), Cols = source (Lead)
    mat_dt <- dcast(plot_data, target_liq_bin ~ source_liq_bin, value.var = "mean_ret1")

    # Convert to pure matrix for plotting
    plot_matrix <- as.matrix(mat_dt[, -1])
    rownames(plot_matrix) <- mat_dt$target_liq_bin

    # 2. Open PNG Device directly (Bypassing ggsave)
    # 5x5 inches at 300 DPI = 1500 pixels. High res.
    filename <- paste0(to_dir, this_idx, "_", plot_data[1, var], ".png")
    png(filename, width = 5.5, height = 5.5, units = "in", res = 300)

    # 3. Create Plot
    # par(oma...) adds Outer Margins to ensure labels are never cut off
    par(oma = c(2.5, 2.5, 2.5, 2.5))

    corrplot(plot_matrix,
      method = "color", # Use colored tiles
      is.corr = FALSE, # Tell R these aren't correlations (range > 1 or < -1 is allowed)
      col = col_palette, # Apply our Blue-White-Red palette
      cl.lim = limit_range, # Fix the color limits!
      cl.pos = "n", # "n" = No Color Legend (as you requested)
      addCoef.col = "black", # Color of the numbers inside tiles
      number.cex = 1.5, # Font size of the numbers
      tl.col = "black", # Color of axis labels
      tl.cex = 1.2, # Font size of axis labels
      tl.srt = 0, # Text rotation (0 = horizontal)
      outline = TRUE # Add white grid lines
    )

    # Add Axis Labels manually (Corrplot doesn't natively label X and Y distinct titles)
    mtext("Lead Liquidity Tercile", side = 1, line = 2.5, cex = 1.2, font = 2, padj = 4)
    mtext("Lag Liquidity Tercile", side = 2, line = 2.5, cex = 1.2, font = 2, padj = -2.5)

    # Close the file
    dev.off()
  }
}

# -- for excel (just transpose it)
subdata <- data[var_type == "asy"]
d <- data.table()
for (i in 2:9) {
  d <- rbind(d, dcast(subdata[var_idx == i], source_liq_bin ~ target_liq_bin, value.var = "mean_ret1"))
}
d
