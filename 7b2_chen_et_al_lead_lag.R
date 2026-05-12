# --- This is the crucial script. For each period t, figure out what valuation would have been if we use q from a *previous* period
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

# we already solved for icc for each period
data <- readRDS("tmp/valuation/contemporaneous.RDS")
data[, idx := frank(statpers, ties.method = "dense")]

# get lagged q for up to 10 years
hh <- 0:40
for (i in setdiff(hh, 0)) {
  data <- merge(data, data[, list(idx = idx + i, permno, xx = q)], by = c("idx", "permno"), all.x = T)
  setnames(data, "xx", paste0("q_", i))
}
data[, idx := NULL]
setnames(data, "q", "q_0")

# TODO: be closer to Chen et al's method to use both q[t] and q[t-lag] to compute CF return.
# compute valuation using eps[t] and q[t-lag]
p.getOne <- function(tt) {
  # == get CF first
  # first two years
  ee <- tt[, c(eps1, eps2)]

  # years 3 to 16
  gg <- seq(from = tt[, log(1 + ltg / 100)], to = tt[, log(1 + ltg_ind / 100)], length.out = 14) # log growth rate
  ee <- c(ee, ee[2] * exp(cumsum(gg)))

  # subsequent years. Project out to 50 years
  ee <- c(ee, last(ee) * exp(c(1:34) * tt[, log_gdp_growth_rate]))

  # various discounts
  out <- data.table()
  for (i in hh) {
    q <- as.numeric(tt[, paste0("q_", i), with = F])
    df <- 1 / ((1 + q)^c(1:50)) * tt[1, payout_ratio] # discount factor
    out <- rbind(out, data.table(lag = i, valuation = sum(ee * df)))
  }

  out[, idx := tt[1, idx]]
  return(out)
}

# split into chunks and execute
blocks <- 30
data[, idx := .I]
data[, block_idx := ntile(idx, blocks)]
nc <- detectCores() - 1

# Takes around half an hour w/ 7 cores
out <- data.table()
plan(multisession, workers = nc)
for (i in 1:blocks) {
  tic(i / blocks)
  data_list <- split(data[block_idx == i], by = "idx")
  out <- rbind(out, rbindlist(future_lapply(data_list, p.getOne, future.seed = TRUE, future.packages = c("data.table"), future.globals = c("p.getOne", "hh"))))
  gc()
  toc()
}
plan(sequential)

out <- merge(out, data[, .(idx, statpers, permno)], by = "idx")[, idx := NULL]

# put together and get row identities
to_file <- "tmp/valuation/using_previous_icc_more_lags.RDS"
dir.create(dirname(to_file), recursive = TRUE, showWarnings = FALSE)
saveRDS(out, to_file)
