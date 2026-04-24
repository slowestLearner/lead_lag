# -- Separate 1m CSM profitability by 3x3 liquidity bins
# form combined signal by adding up individual ones
library(sandwich)
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")


# -- get split signals
# this_type <- "signal_by_liquidity_within_row_of_sub_matrices"
# this_type <- "signal_by_liquidity_within_row_of_pi_matrix"
this_type <- "signal_by_liquidity"
if (.Platform$OS.type == "windows") {
  base_dir <- "D:/Dropbox/JD/"
} else {
  base_dir <- "~/Dropbox/JD/"
}

from_dir <- file.path(base_dir, "data", this_type, "liq_var_minus_spread/")
file_map <- c(
  "Analyst.Rds"     = "analyst",
  "BEAcustomer.Rds" = "beacustomer",
  "BEAsupplier.Rds" = "beasupplier",
  "Econ.Rds"        = "econ",
  "Geo.Rds"         = "geo",
  "Indu.Rds"        = "industry",
  "Pseudo.Rds"      = "pseudo",
  "Tec.Rds"         = "tech"
)

# -- load data
tic("loading data")
data_list <- lapply(names(file_map), function(f_name) {
  f_path <- file.path(from_dir, f_name)

  if (file.exists(f_path)) {
    dt <- readRDS(f_path)
    dt[, var := file_map[[f_name]]]
    return(dt)
  } else {
    warning(paste("File missing:", f_path))
    return(NULL)
  }
})

data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
rm(data_list)
gc()
toc()

# Construct combined and save
combined_data <- data[, .(
  var = "combined",
  signal_a_bin1 = mean(signal_a_bin1),
  signal_a_bin2 = mean(signal_a_bin2),
  signal_a_bin3 = mean(signal_a_bin3),
  signal_s_bin1 = mean(signal_s_bin1),
  signal_s_bin2 = mean(signal_s_bin2),
  signal_s_bin3 = mean(signal_s_bin3)
), by = .(yyyymm, permno)]

saveRDS(combined_data, file.path(from_dir, "combined.Rds"))
