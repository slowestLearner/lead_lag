# --- move data from our own computers to this folder
library(this.path)
setwd(this.path::this.dir())
source("../runmefirst.R")
# All the data are copied from JL
# except quarterly_eps_ltg_price.RDS is downloaded from WRDS using SAS by JD


# -- historical gdp growth rate
data <- readRDS("../../../../extrapolating_from_price/tests/before_2025/27_explanatory_power_bgls/tmp/others/gdp_growth_rate.RDS")
saveRDS(data, "tmp/raw_data/gdp_growth_rate.RDS")

# -- monthly returns
data <- readRDS("../../../../../../Desktop/J-Leaves/data/stockprices/raw/monthly/msf_cleaned/2024.RDS")
data <- data[, .(yyyymm, permno, siccd = as.integer(siccd), ret, prc, shrout)]

to_dir <- "tmp/raw_data/"
dir.create(to_dir, recursive = T, showWarnings = FALSE)
saveRDS(data, paste0(to_dir, "monthly_stock_prices.RDS"))


# -- FF12 industry definition
data <- readRDS("../../../../../../Desktop/J-Leaves/data/portfolios/industries/12ind/industryDef.RDS")
saveRDS(data, "tmp/raw_data/ff_12ind_definition.RDS")

# -- raw compustat annual
data <- readRDS("../../../../../../Desktop/J-Leaves/data/fundamental/raw/ccmfunda/20250515.RDS")
data <- data[, .(
  fyear, gvkey,
  permno,
  sich = as.integer(sich), at, ib, dvc, sstk, prstkc,
  prcc_c, prcc_f, csho
)]

# all needs to be numerical
vars <- c("at", "ib", "dvc", "sstk", "prstkc", "prcc_f", "prcc_c", "csho")
data[, (vars) := lapply(.SD, as.numeric), .SDcols = vars]
rm(vars)

saveRDS(data, "tmp/raw_data/compustat_annual.RDS")


# Have checked how well my data quarterly_eps_ltg_price.RDS matches with
# jl's. They match pretty well. The comparison code can be found at
# tests\28_analyst\jl_modified\1_Compare_IBES_Data.R

jd_data <- read_sas("../../data/ibes/final_dataset.sas7bdat") %>%
  rename_with(tolower) %>%
  dplyr::transmute(statpers, permno, ticker, fpi = as.integer(fpi), est, price, cfacshr) %>%
  dplyr::arrange(permno, statpers, fpi) %>%
  setDT()
jd_data[, cfacshr := NULL]

jd_data[fpi == 0, `:=`(
  est = Winsorize(est, probs = c(0.001, 0.999), na.rm = TRUE)
), by = .(fpi)]
# The above winsorization process will make the data match JL's perfectly.
saveRDS(jd_data, file = "../../data/ibes/quarterly_eps_ltg_price.RDS")



# --- eps, ltg, and price data aligned, JL's code
# data <- readRDS("../../../../extrapolating_from_price/tests/before_2025/28.5_chen_redone/tmp/raw_data/quarterly_eps_ltg_price.RDS")
# saveRDS(data, "tmp/raw_data/quarterly_eps_ltg_price.RDS")
#
# data <- readRDS("tmp/raw_data/quarterly_eps_ltg_price.RDS") %>%
#   dplyr::arrange(permno, statpers, fpi)
