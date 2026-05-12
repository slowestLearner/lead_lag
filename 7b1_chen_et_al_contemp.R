# --- Solve for q (discount rate) under Chen et al methodology
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


# --- first put together all relevant data

# earnings and LTG
data <- readRDS("../../data/ibes/quarterly_eps_ltg_price.RDS") # this is at the end of the quarter (statpers)

# put into wide format. NOTE: I always express eps1 and eps2 as % of price in this period... this is just a normalization
out <- data[, list(price = last(price), ticker = last(ticker)), list(statpers, permno)]
out <- merge(out, data[fpi == 1, list(statpers, permno, eps1 = est / price)],
  by = c("statpers", "permno")
)
out <- merge(out, data[fpi == 2, list(statpers, permno, eps2 = est / price)],
  by = c("statpers", "permno")
)
out <- merge(out, data[fpi == 0, list(statpers, permno, ltg = est)], by = c("statpers", "permno"))
data <- copy(out)
rm(out)
data <- data[(eps2 > .01) & (eps2 < .5)] # negative eps2 does not work with this methodology
data[, yyyymm := floor(statpers / 100)]

# merge with industry-level ltg
tmp <- readRDS("tmp/inputs/ind12_assignment_and_me_by_quarter.RDS")
tmp <- tmp[, list(yyyymm, permno, ind)]
data <- merge(data, tmp, by = c("yyyymm", "permno"))
rm(tmp)

tmp <- readRDS("tmp/raw_data/ind_level/industry_level_ltg.RDS")
tmp <- tmp[, list(yyyymm, ind, ltg_ind = ltg_vw)]
data <- merge(data, tmp, by = c("yyyymm", "ind"))
rm(tmp)

# also get industry-specific payout ratio
tmp <- readRDS("tmp/inputs/payout_ratio_by_ind.RDS")
data <- merge(data, tmp, by = "ind")
rm(tmp)

# add historical gdp rate. This is the terminal growth rate
tmp <- readRDS("tmp/raw_data/gdp_growth_rate.RDS")
tmp <- tmp[, list(yyyy, log_gdp_growth_rate = log_growth_rate)]
data[, yyyy := floor(statpers / 10000)]
data <- merge(data, tmp, by = "yyyy")
rm(tmp)
data[, yyyy := NULL]
data[, idx := .I]

# function to solve for q (icc) and to breakdown valuation into a few components
# key:
p.get_one <- function(tt) {
  # first two years
  ee <- tt[, c(eps1, eps2)]

  # years 3 to 16
  gg <- seq(from = tt[, log(1 + ltg / 100)], to = tt[, log(1 + ltg_ind / 100)], length.out = 14) # log growth rate
  ee <- c(ee, ee[2] * exp(cumsum(gg)))

  # subsequent years. Project out to 50 years
  ee <- c(ee, last(ee) * exp(c(1:34) * tt[, log_gdp_growth_rate]))


  # solve for discount rate
  p.obj <- function(q) {
    df <- 1 / ((1 + q)^c(1:50)) # discount factors
    xx <- sum(df * ee) * tt[1, payout_ratio]
    return((xx - 1)^2)
  }

  q <- optimize(p.obj, c(0, 1))$min

  # vector of discount factor by horizon
  df <- 1 / ((1 + q)^c(1:50)) * tt[1, payout_ratio]

  # in addition to q (icc), also update the present value of earnings by horizon. For instance, valuation_3to10y is the sum of the present value of earnings from year 3 to 10.
  out <- data.table(
    idx = tt[1, idx],
    q,
    valuation_1y = ee[1] * df[1],
    valuation_2y = ee[2] * df[2],
    valuation_3to10y = sum(ee[3:10] * df[3:10]),
    valuation_11to15y = sum(ee[11:15] * df[11:15]),
    valuation_16y_and_on = sum(ee[16:50] * df[16:50]),
    valuation = sum(ee * df)
  )
  return(out)
}

# --- split the data into blocks and execute. for me, 7 cores take around 6-7 mins

blocks <- 30
data[, idx := .I]
data[, block_idx := ntile(idx, blocks)]


data <- data[ltg > -99 & !is.na(ltg)]
# Also check industry ltg just in case
data <- data[ltg_ind > -99 & !is.na(ltg_ind)]

# takes around 10 mins
out <- data.table()
plan(multisession, workers = 7)
for (i in 1:blocks) {
  tic(i / blocks)
  data_list <- split(data[block_idx == i], by = "idx")
  out <- rbind(out, rbindlist(future_lapply(data_list, p.get_one, future.seed = TRUE, future.packages = c("data.table"), future.globals = FALSE)))
  gc()
  toc()
}
plan(sequential)

# put all columns together and save
stopifnot(1 == nrow(out) / nrow(data))
out <- merge(out, data, by = "idx")[, c("idx", "block_idx") := NULL]

to_dir <- "tmp/valuation/"
dir.create(to_dir, recursive = T, showWarnings = FALSE)
saveRDS(out, paste0(to_dir, "contemporaneous.RDS"))
