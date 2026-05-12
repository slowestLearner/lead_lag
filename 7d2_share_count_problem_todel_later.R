# if share count is the issue, then using this IBES file to compute return will automatically become a problem and have overly low volatility for longer horizons
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

data <- readRDS("../../data/ibes/quarterly_eps_ltg_price.RDS")
data <- data[, .(price = last(price)), .(statpers, permno)] %>%
  mutate(yyyymm = floor(statpers / 100)) %>%
  select(-statpers)

# get future cumulative returns
data[, idx := frank(yyyymm, ties.method = "dense")]
for (i in 1:40) {
  data <- merge(data, data[, .(idx = idx - i, permno, xx = price)], by = c("idx", "permno"), all.x = T)
  setnames(data, "xx", paste0("price", i))
}

for (i in 1:40) {
  setnames(data, paste0("price", i), "price_future")
  data[, ret := log(price_future / price)]
  setnames(data, "ret", paste0("ret", i))
  data[, price_future := NULL]
}
data[, c("idx", "price") := NULL]

data <- melt(data, id.vars = c("yyyymm", "permno"), variable.name = "hor", value.name = "ret") %>%
  mutate(hor := as.integer(gsub("ret", "", as.character(hor)))) %>%
  na.omit() %>%
  setDT()

data[, sd(ret), hor]

# --- is it due to not having dividends?

# aggregate to quarterly return
data <- readRDS("../../../../../Desktop/J-Leaves/data/stockprices/raw/quarterly/from_msf_cleaned/2024.RDS")[shrcd %in% 10:12, .(yyyymm, permno, ret = retx)] %>%
  na.omit() %>%
  setDT()
data[, ret := Winsorize(ret, quantile(ret, probs = c(0.001, 0.999)))]

# get future cumulative return without dividends
data[, idx := frank(yyyymm, ties.method = "dense")]
for (i in 1:40) {
  data <- merge(data, data[, .(idx = idx - i, permno, xx = ret)], by = c("idx", "permno"), all.x = T)
  setnames(data, "xx", paste0("ret", i))
}
for (i in 2:40) {
  setnames(data, paste0("ret", c(i - 1, i)), c("xx", "yy"))
  data[, yy := (1 + xx) * (1 + yy) - 1]
  setnames(data, c("xx", "yy"), paste0("ret", c(i - 1, i)))
}
data[, c("idx", "ret") := NULL]

data <- melt(data, id.vars = c("yyyymm", "permno"), variable.name = "hor", value.name = "ret") %>%
  mutate(hor := as.integer(gsub("ret", "", as.character(hor)))) %>%
  na.omit() %>%
  setDT()

data[, sd(ret), hor]
