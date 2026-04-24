library(this.path)
library(data.table)
library(future.apply)
library(rootSolve)
library(haven)
library(dplyr)
library(lubridate) # Essential for date math

setwd(this.path::this.dir()) 
source("runmefirst.R")

data <- readRDS('../../tests/27_sym_mechanism_tests/tmp/raw_data/ltg_annual_forecasts_10y.RDS') %>%
  mutate(yyyyqq = floor(statpers / 100)) %>%
  select(-statpers) %>%
  melt(id.vars = c("yyyyqq", "permno", "estimator"), variable.name = "est_hor", value.name = "ltg") %>%
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
data <- merge(data, data[hor == -1, .(yyyyqq, permno, estimator, ltg_1 = ltg)], by = c("yyyyqq", "permno", "estimator")) %>%
  mutate(dltg = ltg - ltg_1) %>%
  select(-ltg_1, -ltg)

# average over estimators (analysts)
data <- data[, .(dltg = mean(dltg)), .(yyyyqq, permno, hor)] %>% filter(yyyyqq >= 198209)
ltg_data <- copy(data)
rm(data)
gc()

# slightly winsorize by horizon
ltg_data[, dltg := Winsorize(dltg, quantile(dltg, probs = c(.005, .995))), .(yyyyqq, hor)]

# convert date to yyyymm
tmp <- data.table(yyyymm = unlist(lapply(1982:2024, function(x) {
  100 * x + 1:12
}))) %>%
  mutate(mm = yyyymm - 100 * floor(yyyymm / 100), qq = ceiling(mm / 3) * 3, yyyyqq = yyyymm + qq - mm) %>%
  select(-qq, -mm)
ltg_data <- merge(ltg_data, tmp, by = "yyyyqq", allow.cartesian = TRUE) %>% select(-yyyyqq)
rm(tmp)


head(ltg_data)
ltg_data_jc_0 <- ltg_data %>%
  dplyr::filter(hor %in% c(0, 1, 2))

ltg_data_jd_old <- readRDS("../../data/Analyst/ltg_chg.Rds") %>%
  dplyr::transmute(permno, yyyymm, ltg_chg_old = ltg_chg)

ltg_data_jd <- readRDS( paste0("../../data/Analyst/", "ltg_chg_detail.Rds")) %>%
  dplyr::transmute(permno, yyyymm = year(date)*100 + month(date), ltg_chg)

ltg_comparison <- ltg_data_jd_old %>%
  dplyr::inner_join(ltg_data_jd, by = c("permno", "yyyymm"))

df_summary <- ltg_comparison %>%
  dplyr::group_by(yyyymm) %>%
  dplyr::summarise(cor_e = cor(ltg_chg_old, ltg_chg, method = "spearman")) %>%
  dplyr::ungroup()

ggplot(df_summary , aes(x = yyyymm, y = cor_e)) +
  geom_line(color = "steelblue", alpha = 0.5) +
  #facet_wrap(~hor, scales = "free")+
  theme_bw()
