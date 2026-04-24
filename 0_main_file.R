library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")



source("2a1_get_residuals_of_csm_signals.R")

source("2a2_get_residuals_of_csm_signals_super_set.R")


source("2a3_Burt_ Hrdlicka_Adjust.R")

source("2b1_reversals_only_total_different_specs.R")

source("2c1_reversals_tstats_only_total_different_specs.R")


source("2c2_reversals_tstats_nw_include_sym_as.R")

source("2d_reversals_tstats_nw_fraction_cov.R")
