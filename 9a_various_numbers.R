# --- Getting various one-off numbers
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

# --- fraction of stocks lacking delisting returns
tmp <- readRDS("../../data/Stocks/Monthly_CRSP_delisting.RDS")
tmp[keep == 1, mean(is.na(ret)), ret_type]

tmp[keep == 1 & is.na(ret)]
data[yyyymm == 202412 & permno == 10026]
