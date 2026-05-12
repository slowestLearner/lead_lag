library(this.path)
library(data.table)
library(future.apply)
library(rootSolve)
library(haven)
library(dplyr)
library(lubridate) # Essential for date math

setwd(this.path::this.dir()) 
source("runmefirst.R")

# Load SAS Data
dt <- read_sas("../../data/Analyst/det_ltg_raw.sas7bdat") %>%
  rename_all(tolower) %>%
  dplyr::filter(fpi == "0") %>%
  dplyr::select(permno, estimator, ltg = value, anndats, analys) %>%
  setDT()
# map each update to latest month, jc map to latest quarter
tmp <- unique(dt[, .(anndats)])
tmp[, yyyymm := 100 * year(anndats) + month(anndats)]


dt <- merge(dt, tmp, by = "anndats")
rm(tmp)

dt <- dt[order(permno, estimator, analys, yyyymm, anndats)]
#estimator refers to the broker
#analys refers to the specific analyst
dt <- dt[, .(ltg = last(ltg)), .(yyyymm, permno, estimator, analys)]
#aggregate from broker-analyst level to broker level
dt <- dt[, .(ltg = mean(ltg)), .(yyyymm, permno, estimator)]
#deal with outliers
#hist(dt[,ltg])
winsor_p <- 0.005
bounds <- quantile(dt$ltg, probs = c(winsor_p, 1-winsor_p), na.rm = TRUE)
dt[, ltg := pmin(pmax(ltg, bounds[1]), bounds[2])]
setorder(dt, permno, yyyymm, estimator)
to_dir <- "tmp/analystjd/"
dir.create(to_dir, recursive = T, showWarnings = FALSE)
saveRDS(dt, paste0(to_dir, "ltg_latest_by_month.RDS"))


dt <- readRDS(paste0(to_dir, "ltg_latest_by_month.RDS"))




time_data <- unique(dt[, .(yyyymm)])[order(yyyymm)] %>%
  mutate(idx = row_number()) %>%
  setDT()

data <- dt[time_data, on = "yyyymm", nomatch = NULL]
setnames(data, "ltg", "est")


# fill forward
for (i in 1:(8*3)) {
  print(i)
  data <- merge(data, data[, .(idx = idx + i, permno, estimator, xx = est)], by = c("idx", "permno", "estimator"), all.x = T)
  setnames(data, "xx", paste0("est_", i))
}
for (i in 1:(40*3)) {
  print(i)
  data <- merge(data, data[, .(idx = idx - i, permno, estimator, xx = est)], by = c("idx", "permno", "estimator"), all.x = T)
  setnames(data, "xx", paste0("est", i))
}
setnames(data, "est", "est0")

# fill forward
for (i in (8*3):2) {
  print(i)
  setnames(data, paste0("est_", c(i, i - 1)), c("xx", "yy"))
  data[is.na(yy), yy := xx]
  setnames(data, c("xx", "yy"), paste0("est_", c(i, i - 1)))
}

for (i in 1:(40*3)) {
  print(i)
  setnames(data, paste0("est", c(i - 1, i)), c("xx", "yy"))
  data[is.na(yy), yy := xx]
  setnames(data, c("xx", "yy"), paste0("est", c(i - 1, i)))
}
data <- data[time_data, on = "yyyymm", nomatch = NULL]

data[, idx := NULL]


data <- data %>%
  melt(id.vars = c("yyyymm", "permno", "estimator"), variable.name = "est_hor", value.name = "ltg") %>%
  mutate(est_hor = as.character(est_hor), ltg = ltg / 100) %>%
  na.omit() %>%
  setDT()

# parse horizon
tmp <- unique(data[, .(est_hor)]) %>%
  mutate(hor = gsub("est", "", est_hor)) %>%
  mutate(hor = as.integer(gsub("_", "-", hor)))
data <- merge(data, tmp, by = "est_hor") %>%
  select(-est_hor) %>%
  filter(hor >= -1)
rm(tmp)

# get cumulative changes from hor = -1
data <- merge(data, data[hor == -1, .(yyyymm, permno, estimator, ltg_1 = ltg)], 
              by = c("yyyymm", "permno", "estimator")) %>%
  mutate(dltg = ltg - ltg_1) %>%
  select(-ltg_1, -ltg)

# average over estimators (analysts)
data <- data[, .(dltg = mean(dltg)), .(yyyymm, permno, hor)] 
ltg_data <- copy(data)
rm(data)
gc()

#hist(ltg_data[, dltg])
# slightly winsorize by horizon
p <- 0.005

# Fast Grouped Winsorization
ltg_data[, dltg := {
  qs <- quantile(dltg, probs = c(p, 1 - p), na.rm = TRUE)
  pmin(pmax(dltg, qs[1]), qs[2])
}, by = .(yyyymm, hor)]

start_ym <- min(ltg_data[, yyyymm])

data <- readRDS("../../../JD/data/signal_demean/archive/Analyst.Rds")[, var := "analyst"]
data <- rbind(data, readRDS("../../../JD/data/signal_demean/archive/BEAcustomer.Rds")[, var := "beacustomer"])
data <- rbind(data, readRDS("../../../JD/data/signal_demean/archive/BEAsupplier.Rds")[, var := "beasupplier"])
data <- rbind(data, readRDS("../../../JD/data/signal_demean/archive/Econ.Rds")[, var := "econ"])
data <- rbind(data, readRDS("../../../JD/data/signal_demean/archive/Geo.Rds")[, var := "geo"])
data <- rbind(data, readRDS("../../../JD/data/signal_demean/archive/Indu.Rds")[, var := "industry"])
data <- rbind(data, readRDS("../../../JD/data/signal_demean/archive/Pseudo.Rds")[, var := "pseudo"])
data <- rbind(data, readRDS("../../../JD/data/signal_demean/archive/Tec.Rds")[, var := "tech"])
setnames(data, "signal_s", "signal_sym")
setnames(data, "signal_a", "signal_asy")
data[, signal_total := signal_sym + signal_asy]
data <- data %>% filter(yyyymm >= start_ym)


data <- melt(data, id.vars = c("yyyymm", "permno", "var"), variable.name = "var_type", value.name = "signal") %>%
  mutate(var_type = as.character(var_type)) %>%
  setDT()
data[, var_type := gsub("signal_", "", var_type)]

# get combined signal
data_combined <- data[, .(var = "combined", signal = mean(signal)), .(yyyymm, permno, var_type)]
# signal_data <- rbind(data, data_combined)
signal_data <- rbind(data_combined)
rm(data, data_combined)
gc()

# -- merge and compute cumulative LTG changes

# let's figure this out by horizon
#ltg_data[, date := as.Date(paste0(yyyymm, "01"), format = "%Y%m%d")]

unique_yyyymm <- ltg_data[, .(yyyymm = unique(yyyymm))]

# 2. Convert only the unique values to Date
unique_yyyymm[, date := as.Date(paste0(yyyymm, "01"), format = "%Y%m%d")]

# 3. Update the original table using a fast join (the "update-on-join" syntax)
ltg_data[unique_yyyymm, on = .(yyyymm), date := i.date]

hors <- unique(ltg_data[, hor]) %>% sort()

ltg_list <- split(ltg_data, by = "hor")

# 2. Set the limit slightly higher just for the signal_data
options(future.globals.maxSize = 4000 * 1024^2) 

plan(multisession, workers = detectCores() - 12)

# 3. Pass the signal_data explicitly and iterate over the list
out_list <- future_lapply(ltg_list, function(sub_ltg) {
  # sub_ltg is now only the slice for ONE horizon
  this_hor <- sub_ltg$hor[1]
  
  # Join with signal_data (still a global, but now we only have ONE large global)
  data <- signal_data[sub_ltg, on = .(yyyymm, permno), nomatch = NULL]
  data <- na.omit(data)
  
  if(nrow(data) == 0) return(NULL)
  
  # Scaling logic
  data[, sum_abs := sum(abs(signal)), by = .(yyyymm, var)]
  data[, signal := 2 * signal / sum_abs]
  
  res <- data[, .(dltg = sum(dltg * signal)), by = .(yyyymm, var, var_type)]
  res[, hor := this_hor]
  
  return(res)
}, future.seed = TRUE)
plan(sequential)
out_all <- rbindlist(out_list)

to_dir <- "tmp/analystjd/"
dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
saveRDS(out_all, paste0(to_dir, "dltg_cumulative_detail.RDS"))

out_all <- readRDS(paste0(to_dir, "dltg_cumulative_detail.RDS"))

this_var_type <- "sym"
# this_var_type <- "total"
# this_var_type <- "asy"
out <- copy(out_all[var_type == this_var_type])
out <- out[, .(dltg = mean(dltg)), .(hor, var)]

# # plot
ggplot(out, aes(x = hor / 12, y = dltg, color = var)) +
  geom_line(lwd = 1) +
  geom_hline(yintercept = 0, lty = 3) +
  theme_classic() +
  labs(x = "horizon (years)", y = "cumulative change in LTG") +
  ggtitle(paste0("var_type = ", this_var_type)) +
  theme(legend.position = c(.8, .85), 
        legend.title = element_blank(), plot.title = element_text(hjust = .5), text = element_text(size = 30))

