# --- plowback rate from Compustat: by industry.
library(this.path)
setwd(this.path::this.dir())
source("../runmefirst.R")

# os_type <- Sys.info()["sysname"]

# # Set the file path based on the operating system
# if (os_type == "Windows") {
#   # For Windows

# } else {
#   # For macOS and Linux
#   source("~/.runmefirst")
# }


# get CRSP-compsutat merge, annual data
data <- readRDS("tmp/raw_data/compustat_annual.RDS")[, list(fyear, gvkey,
  permno, sich, at, ib, dvc, sstk, prstkc,
  prcc = ifelse(!is.na(prcc_f), prcc_f, prcc_c), csho
)]
data <- data[!is.na(fyear)]

# compute market cap
data[, me := prcc * csho][, c("prcc", "csho") := NULL]

# some basic filtering
data <- data[!is.na(sich) & (at >= 1) & !is.na(ib) & !is.na(me) & !is.na(dvc)]
data <- data[fyear >= 1987] # use data after 1987
data[is.na(data)] <- 0 # fill in zeros if needed

# compute net buybacks (buyback - issuance)
data[, net_purchase := (prstkc - sstk)][, c("prstkc", "sstk") := NULL]

# map to FF12 industries using historical SIC code
tmp <- readRDS("tmp/raw_data/ff_12ind_definition.RDS")
for (i in 1:nrow(tmp)) {
  data[(sich %in% c(tmp[i, sicStart]:tmp[i, sicEnd])), ind := tmp[i, ind]]
}
data[is.na(ind), ind := "other"]
rm(i, tmp)

# turn earnings, dividends, and net buybacks as a fraction of total assets (at), and winsorize
vv <- c("ib", "dvc", "net_purchase")
for (this in vv) {
  setnames(data, this, "xx")
  data[, xx := xx / at]
  data[, xx := Winsorize(xx, quantile(xx, probs = c(.005, .995)))]
  setnames(data, "xx", this)
}

# sum up by industry and year
out <- data[, list(
  at = sum(at), ib = weighted.mean(ib, at),
  dvc = weighted.mean(dvc, at),
  net_purchase = weighted.mean(net_purchase, at)
), list(fyear, ind)]
out[, net_payout := dvc + net_purchase]

# get payout ratio and output
tt <- out[, list(payout_ratio = sum(net_payout) / sum(ib)), ind]
tt <- tt[order(payout_ratio)]

# save payout by industry
to_dir <- "tmp/inputs/"
dir.create(to_dir, recursive = T, showWarnings = FALSE)
saveRDS(tt, paste0(to_dir, "payout_ratio_by_ind.RDS"))
