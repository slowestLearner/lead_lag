# --- compute industry assignments for stocks
library(this.path)
setwd(this.path::this.dir())
os_type <- Sys.info()["sysname"]

# Set the file path based on the operating system
if (os_type == "Windows") {
  # For Windows
  source("../runmefirst.R")
} else {
  # For macOS and Linux
  source("~/.runmefirst")
}


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

# figure out industry assignments
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


# # --- 2) Check plowback rate from Compustat: by industry.
# library(this.path)
# setwd(this.path::this.dir())
# source("../runmefirst.R")


# # get CRSP-compsutat merge, annual data
# data <- readRDS("../../../../../../Desktop/J-Leaves/data/fundamental/raw/ccmfunda/20250515.RDS")
# data <- data[, list(fyear, gvkey,
#   permno, sich, at, ib, dvc, sstk, prstkc,
#   prcc = ifelse(!is.na(prcc_f), prcc_f, prcc_c), csho
# )]
# data <- data[!is.na(fyear)]

# # all needs to be numerical
# data[, sich := as.integer(sich)]
# vars <- c("at", "ib", "dvc", "sstk", "prstkc", "prcc", "csho")
# data[, (vars) := lapply(.SD, as.numeric), .SDcols = vars]
# rm(vars)

# data[, me := prcc * csho] # market cap of companies
# data[, c("prcc", "csho") := NULL]
# data <- data[!is.na(sich) & (at >= 1) & !is.na(ib) & !is.na(me) & !is.na(dvc)]
# data <- data[fyear >= 1987] # use data after 1987
# data[is.na(data)] <- 0 # fill in zeros if needed

# # compute net buybacks (buyback - issuance)
# data[, net_purchase := (prstkc - sstk)][, c("prstkc", "sstk") := NULL]

# # map to FF12 industries using historical SIC code
# tmp <- readRDS("../../../../../../Desktop/J-Leaves/data/portfolios/industries/12ind/industryDef.RDS")
# for (i in 1:nrow(tmp)) {
#   data[(sich %in% c(tmp[i, sicStart]:tmp[i, sicEnd])), ind := tmp[i, ind]]
# }
# data[is.na(ind), ind := "other"]
# data_bk <- copy(data)
# rm(i, tmp)


# # turn earnings, dividends, and net buybacks as a fraction of total assets (at), and winsorize
# vv <- c("ib", "dvc", "net_purchase")
# for (this in vv) {
#   setnames(data, this, "xx")
#   data[, xx := xx / at]
#   data[, xx := Winsorize(xx, quantile(xx, probs = c(.005, .995)))]
#   setnames(data, "xx", this)
# }

# # sum up by industry and year
# out <- data[, list(
#   at = sum(at), ib = weighted.mean(ib, at),
#   dvc = weighted.mean(dvc, at),
#   net_purchase = weighted.mean(net_purchase, at)
# ), list(fyear, ind)]
# out[, net_payout := dvc + net_purchase]

# # get payout ratio and output
# tt <- out[, list(payout_ratio = sum(net_payout) / sum(ib)), ind]
# tt <- tt[order(payout_ratio)]

# to_dir <- "tmp/inputs/"
# dir.create(to_dir, recursive = T, showWarnings = FALSE)
# saveRDS(tt, paste0(to_dir, "payout_ratio_by_ind.RDS"))
