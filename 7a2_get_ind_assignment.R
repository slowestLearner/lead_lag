# --- compute industry assignments for stocks
library(this.path)
setwd(this.path::this.dir())
# os_type <- Sys.info()["sysname"]
source("../runmefirst.R")

# # Set the file path based on the operating system
# if (os_type == "Windows") {
#   # For Windows
#   source("../runmefirst.R")
# } else {
#   # For macOS and Linux
#   source("~/.runmefirst")
# }


# monthly stock data
data <- readRDS("tmp/raw_data/monthly_stock_prices.RDS")[, .(yyyymm, permno, siccd, me = prc * shrout / 1e3)] %>% na.omit()

# turn to quarterly frequency
tmp <- unique(data[, list(yyyymm)])
tmp[, mm := yyyymm - 100 * floor(yyyymm / 100)]
tmp[, qq := ceiling(mm / 3) * 3]
tmp <- tmp[, list(yyyymm, yyyyqq = yyyymm + (qq - mm))]
data <- merge(data, tmp, by = "yyyymm")
data <- data[order(yyyymm)]
data <- data[, list(siccd = last(siccd), me = last(me)), list(yyyyqq, permno)]
setnames(data, "yyyyqq", "yyyymm")
rm(tmp)

# figure out FF12 industry assignments
tmp <- readRDS("tmp/raw_data/ff_12ind_definition.RDS")
for (i in 1:nrow(tmp)) {
  print(i / nrow(tmp))
  data[siccd %in% c(tmp[i, sicStart]:tmp[i, sicEnd]), ind := tmp[i, ind]]
}
data[is.na(ind), ind := "other"]
rm(i, tmp)

# save locally
to_dir <- "tmp/inputs/"
dir.create(to_dir, recursive = T, showWarnings = FALSE)
saveRDS(data, paste0(to_dir, "ind12_assignment_and_me_by_quarter.RDS"))
