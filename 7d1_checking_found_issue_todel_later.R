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

# ibes-implied stock returns (next-period return)
data <- readRDS("tmp/valuation/implied_forward_cf_return.RDS")
data[, yyyymm := floor(statpers / 100)]

# aggregate to quarterly return
tmp <- readRDS("../../data/Stocks/Monthly_CRSP.RDS")[, .(yyyymm, permno, ret)]
tmp[, mm := yyyymm %% 100]
tmp[, qq := ceiling(mm / 3) * 3]
tmp[, yyyyqq := yyyymm + (qq - mm)]
tmp <- tmp[, .(obs = .N, ret = prod(1 + ret) - 1), .(yyyyqq, permno)] %>%
  filter(obs == 3) %>%
  select(-obs) %>%
  na.omit()

tmp[, ret := Winsorize(ret, quantile(ret, probs = c(0.001, 0.999)))]

# make next period
tt <- unique(tmp[, .(yyyyqq)])
tt[, qq := yyyyqq %% 100]
tt[, yyyymm := ifelse(qq == 3, yyyyqq - 100 + 9, yyyyqq - 3)][, qq := NULL]
tmp <- merge(tmp, tt, by = "yyyyqq", all.x = T)[, yyyyqq := NULL]
tmp <- tmp[yyyymm >= 198001]
tmp <- tmp[permno %in% data[, unique(permno)]]

# can cumulative forward
tmp[, idx := frank(yyyymm, ties.method = "dense")]
setnames(tmp, "ret", "ret1")
for (i in 1:39) {
  print(i)
  tmp <- merge(tmp, tmp[, .(idx = idx - i, permno, ret = ret1)], by = c("idx", "permno"), all.x = T)
  setnames(tmp, "ret", paste0("ret", i + 1))
}

for (i in 1:39) {
  setnames(tmp, paste0("ret", c(i, i + 1)), c("xx", "yy"))
  tmp[, yy := (1 + xx) * (1 + yy) - 1]
  setnames(tmp, c("xx", "yy"), paste0("ret", c(i, i + 1)))
}
tmp[, idx := NULL]
tmp_bk <- copy(tmp)

tt <- tmp[permno == first(permno)]
tt[hor == 1][1:3]
tt[hor == 2][1:3]
tt[hor == 3][1:3]

tmp[, ret := Winsorize(ret, quantile(ret, probs = c(0.001, 0.999))), hor]
tmp[, sd(ret), hor]


# to long format
tic()
tmp <- melt(tmp, id.vars = c("yyyymm", "permno"), variable.name = "hor", value.name = "ret") %>%
  mutate(hor := as.integer(gsub("ret", "", as.character(hor)))) %>%
  setDT() %>%
  na.omit()
tmp_bk <- copy(tmp)
toc()


# do they agree?
data[, yyyymm := floor(statpers / 100)]
out <- merge(data, tmp, by = c("yyyymm", "permno", "hor")) %>% na.omit()

feols(logrethat ~ ret | yyyymm, out[hor == 16])

data[, sd(logrethat), hor]

feols(logrethat ~ ret | yyyymm, out)
out[, sd(ret)]
out[, sd(logrethat)]
