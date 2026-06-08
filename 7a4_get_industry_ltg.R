# --- Get industry-level average LTG
library(this.path)
setwd(this.path::this.dir())
source("../runmefirst.R")

# os_type <- Sys.info()["sysname"]

# # Set the file path based on the operating system
# if (os_type == "Windows") {
#   # For Windows
#   source("../runmefirst.R")
# } else {
#   # For macOS and Linux
#   source("~/.runmefirst")
# }

# LTG data
data <- readRDS("../../data/ibes/quarterly_eps_ltg_price.RDS")[fpi == 0]
setnames(data, "est", "ltg")

# merge to industry and stock sizes
tmp <- readRDS("tmp/inputs/ind12_assignment_and_me_by_quarter.RDS")[, siccd := NULL]
data[, yyyymm := floor(statpers / 100)]
data <- merge(data, tmp, by = c("yyyymm", "permno"))
rm(tmp)

# summarize ltg by industry/time, and save
data <- data[
  , .(ltg_ew = mean(ltg), ltg_vw = weighted.mean(ltg, me)),
  .(yyyymm, ind)
]

to_dir <- "tmp/raw_data/ind_level/"
dir.create(to_dir, recursive = T, showWarnings = FALSE)
saveRDS(data, paste0(to_dir, "industry_level_ltg.RDS"))
