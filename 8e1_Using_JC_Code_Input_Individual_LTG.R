# Check if analyst forecasts exhibit interesting patterns
source("../runmefirst.R")
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code

table(ltg_data)

#ltg_data <- readRDS("../../data/Analyst/ltg_chg.Rds")
ltg_data <- readRDS( paste0("../../data/Analyst/", "ltg_chg_detail.Rds")) %>%
  dplyr::transmute(permno, yyyymm = year(date)*100 + month(date), ltg_chg)
# --- merge with signals

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

# melt into long format
data <- melt(data, id.vars = c("yyyymm", "permno", "var"), variable.name = "var_type", value.name = "signal") %>%
  mutate(var_type = as.character(var_type))
data[, var_type := gsub("signal_", "", var_type)]

# get combined signal
data_combined <- data[, .(var = "combined", signal = mean(signal)), .(yyyymm, permno, var_type)]
signal_data <- rbind(data, data_combined)
rm(data, data_combined)
gc()

# -- merge and compute cumulative LTG changes

# let's figure this out by horizon
ltg_data[, date := as.Date(paste0(yyyymm, "01"), format = "%Y%m%d")]
out_all <- data.table()
for (this_hor in c(1:120)) {
  # this_hor <- hors[1]
  tic(paste0("horizon = ", this_hor))
  ltg_hor <- copy(ltg_data)
  ltg_hor[, target_date := date %m-% months(this_hor)]
  
  ltg_hor <- ltg_hor[, .(permno, yyyymm = 100*year(target_date) + month(target_date),
                         ltg_chg)]
  data <- merge(ltg_hor, signal_data[var == "combined",], by = c("yyyymm", "permno")) %>% na.omit()
  
  # need to scale
  scale_data <- data[var_type == "total", .(sum_of_abs_signal = sum(abs(signal))), .(yyyymm, var)]
  data <- merge(data, scale_data, by = c("yyyymm", "var"))
  data[, signal := 2 * signal / sum_of_abs_signal]
  data[, sum_of_abs_signal := NULL]
  
  out_all <- rbind(out_all, data[, .(hor = this_hor, dltg = sum(ltg_chg * signal)), by = c("yyyymm", "var", "var_type")])
  toc()
}
rm(data)
gc()

to_dir <- "tmp/analystjd/"
dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
saveRDS(out_all, paste0(to_dir, "dltg_individual_cumulative.RDS"))

this_var_type <- "sym"
# this_var_type <- "total"
# this_var_type <- "asy"
out <- readRDS(paste0(to_dir, "dltg_individual_cumulative.RDS"))[var_type == this_var_type] %>%
  arrange(var, var_type, yyyymm, hor) %>%
  group_by(var, var_type, yyyymm) %>%
  mutate(dltg = cumsum(dltg)) %>%
  ungroup() %>%
  setDT()
out <- out[, .(dltg = mean(dltg)), .(hor, var)]

# # plot
ggplot(out, aes(x = hor / 12, y = dltg, color = var)) +
  geom_line(lwd = 1) +
  geom_hline(yintercept = 0, lty = 3) +
  theme_classic() +
  labs(x = "horizon (years)", y = "cumulative change in LTG") +
  ggtitle(paste0("var_type = ", this_var_type)) +
  theme(legend.position = c(.8, .85), legend.title = element_blank(), plot.title = element_text(hjust = .5), text = element_text(size = 30))
