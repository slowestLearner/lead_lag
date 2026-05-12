# ---- compute implied CF-justified return from end-of-t to end-of-t+h
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

# recall that valuations are always stated as a fraction of price at that time
data <- readRDS("tmp/valuation/using_previous_icc_more_lags.RDS")[valuation > .05 & valuation < 20] %>% na.omit()

# require valuation to be successful. that is, when lag = 0, we can get "valuation" ~ 1
tmp <- copy(data[lag == 0])
tmp <- tmp[abs(valuation - 1) < .01][, .(statpers, permno)] %>% unique()
data <- merge(data, tmp, by = c("statpers", "permno"))
rm(tmp)

# turn valuation into per-share valuation by multiplying with the price at that time
tmp <- readRDS("../../data/ibes/quarterly_eps_ltg_price.RDS")
tmp <- tmp[, .(price = last(price)), .(statpers, permno)]
data <- merge(data, tmp, by = c("statpers", "permno"))
rm(tmp)
data[, v := price * valuation][, c("valuation", "price") := NULL]
data <- data[v > 1]

# get implied log return from end-of-t to end-of-t+h
data[, idx := frank(statpers, ties.method = "dense")]
hh <- sort(unique(data[, lag]))
hh <- hh[hh > 0]

out <- data[lag == 0, list(statpers, idx, permno, v0 = v)] # current valuation

# merge in future valuation using FUTURE earnings and CURRENT ICC
for (i in hh) {
  print(i)
  out <- merge(out, data[lag == i, list(idx = idx - i, permno, xx = v)], by = c("idx", "permno"), all.x = T)
  setnames(out, "xx", paste0("v", i))
}

# turn valuation changes into log returns
for (i in hh) {
  target_col <- paste0("logrethat", i)

  # 1. Calculate the log return
  # Using pmax to avoid log(0) or log(negative)
  out[, (target_col) := log(get(paste0("v", i)) / v0)]

  # 2. Extract the data for this column
  vals <- out[[target_col]]

  # 3. Calculate bounds ONLY on finite values (excludes NA, NaN, Inf)
  finite_vals <- vals[is.finite(vals)]

  if (length(finite_vals) > 0) {
    bounds <- quantile(finite_vals, probs = c(0.001, 0.999))

    # Assign specific single values to min/max
    low_bound <- bounds[1]
    high_bound <- bounds[2]

    # 4. Apply Winsorization manually or via DescTools::Winsorize
    # We use 'out[[target_col]]' to update the column directly
    # and handle NAs by checking is.finite
    out[is.finite(get(target_col)), (target_col) := {
      x <- get(target_col)
      x[x < low_bound] <- low_bound
      x[x > high_bound] <- high_bound
      x
    }]
  }
}

data <- copy(out)
rm(out, i)
gc()
data[, c("v0", "idx") := NULL]

# put into easier to use format
data <- data.table(melt(data, id.vars = c("statpers", "permno"), variable.name = "hor", value.name = "logrethat"))
data[, hor := as.integer(gsub("logrethat", "", as.character(hor)))]
data <- data %>% na.omit()

# save
to_dir <- "tmp/valuation/"
dir.create(to_dir, recursive = T, showWarnings = FALSE)
saveRDS(data, paste0(to_dir, "implied_forward_cf_return.RDS"))
