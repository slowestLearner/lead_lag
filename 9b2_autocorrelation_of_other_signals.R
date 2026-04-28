# --- Do the same for a bunch of other signals to get a sense
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

# characteristics
data <- readRDS("../../data/Stocks/Characteristics/andrew_chen_characteristics.RDS")
data <- melt(data, id.vars = c("yyyymm", "permno"), variable.name = "var", value.name = "signal") %>%
  mutate(var = as.character(var)) %>%
  na.omit() %>%
  setDT()

data[, idx := frank(yyyymm, ties.method = "dense")]
data <- merge(data, data[, .(idx = idx + 1, permno, var, signal_1 = signal)], by = c("idx", "permno", "var"))
gc()

# estimate autocorrelations
data_list <- split(data, by = "var")

p.get_one <- function(this_data) {
  # this_data <- data_list[[1]]
  this_data[, fe := 1]
  out <- data.table()
  mm <- feols(signal ~ signal_1 | fe, this_data, cluster = ~ yyyymm + permno)
  out <- rbind(out, data.table(spec_idx = 1, fe_type = "none", coef = mm$coefficients[1], se = diag(vcov(mm))))
  mm <- feols(signal ~ signal_1 | yyyymm, this_data, cluster = ~ yyyymm + permno)
  out <- rbind(out, data.table(spec_idx = 2, fe_type = "time FE", coef = mm$coefficients[1], se = diag(vcov(mm))[1]))
  mm <- feols(signal ~ signal_1 | yyyymm + permno, this_data, cluster = ~ yyyymm + permno)
  out <- rbind(out, data.table(spec_idx = 3, fe_type = "time + stock FE", coef = mm$coefficients[1], se = diag(vcov(mm))[1]))
  out[, var := this_data[1, var]]
  return(out)
}

tic()
out <- rbindlist(mclapply(data_list, p.get_one, mc.cores = detectCores() - 2))
toc()

to_file <- "tmp/one_off/characteristics_autocorrelation.RDS"
dir.create(dirname(to_file), showWarnings = F, recursive = T)
saveRDS(out, to_file)
