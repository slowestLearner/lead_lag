# Writing things

- We no longer do unif[-0.5, 0.5] transformation! We are just using raw signals! 
	- The combination is z-scores or no? 
- These are demeaned by period, right? 
	- signal_demean
- 

these are already mean zero, right? how? 

~/Dropbox/SpeculativeIdeas/Leadlag/data/signal_demean/



# Top-Level R Script Summaries (`code_final`)

Skip 4b and 4c, go to 6a

- `0_main_file.R`: Master runner script that sources the main preprocessing and reversal/statistics scripts in sequence.
- `1a_signal_availability.R`: Computes monthly equal- and value-weighted coverage rates of each signal across the stock universe.
	- fixed a bug. The current output in signal_availability.RDS always has “availability = 100%”. Fixed.
- `1b_imputing_predictors.R`
	- NEW
- `2a1_get_residuals_of_csm_signals.R`: Residualizes total CSM signals on characteristics and industry controls, then saves cleaned signal panels.
	- have some differences from earlier signals due to different (i,j) universe requirements
- `2a2_get_residuals_of_csm_signals_super_set.R`: Residualizes CSM signals using a larger literature control set and saves the supersetted residual signals.
	- code can be merged into 2a1? Also, it seems that the “superset” list is already in the chen et al characteristics? (DO LATER)
- `2a3_Burt_Hrdlicka_Adjust.R`: Implements Burt-Hrdlicka style adjustments by first estimating factor residuals and then constructing adjusted signals.
	- NOTE: the first part of the code only runs the regression if factor data is fully available. Is this an issue? You can check "tmp/burt_hrdlicka/CRSP_BH.RDS" and compare with monthly stock returns. They are identical for the first half of the data (where liquidity factor is not present)
- `2b1_reversals_only_total_different_specs.R`: Builds long-run return paths for total-signal portfolios across multiple signal construction specifications.
	- this “this_REMOVE_20PCT_STK” thing in director names is redundant, I removed it.
	- My computer can’t handle much parallel processing on this thing…
	- Stuck here. Relevant plot of NW results probably produced from 2c1
- `2b2_reversals_include_sym_asy.R`: Computes portfolio returns for total, symmetric, and asymmetric signal components over multiple horizons.
	- omitted var = combined, I added it back
- `2b3_survival_rate.R`: Tracks stock survival/continuation within signal-sorted portfolios to assess composition persistence.
	- this should be merged into 2b1 and 2b2? Could just be one of the columns output?
	- I find small differences in output
	- I suggest also having “fraction of portfolio (based on signals)” and “VW portfolio” fraction left. Could be useful down the road
- `2c1_reversals_tstats_only_total_different_specs.R`: Computes Newey-West alphas and standard errors for cumulative total-signal returns by specification.
	- Great, no problem
- `2c2_reversals_tstats_nw_include_sym_asy.R`: Computes Newey-West statistics for cumulative symmetric/asymmetric/total return series over selected horizons.
	- Mostly right, but results have changed a bit... the combined CSM return is stronger, especially in its second part. One possibility is that the earlier version ignored the fact that "combined" is already available and then computed it again by averaging over all
- `2d_reversals_tstats_nw_fraction_cov.R`: Estimates covariance structures needed to decompose symmetric/asymmetric shares of total reversal returns.
	- would be good to also combine with the earlier scripts (e.g. 2c2?)
	- Hmm, for combined, the variation has changed a lot. I think the issue may have been upstream. No idea how to deal with it
- `2d_reversals_tstats_nw_fraction_cov_archive.R`: Archived/older implementation of covariance estimation for symmetric/asymmetric return-fraction decomposition.
	- NOTE: delete this script? 
- `2e_reversals_summarize_fraction.R`: Converts covariance outputs into symmetric-to-total and asymmetric-to-total return ratios with delta-method standard errors.
	- Great, no problem
- `3a_sym_corr_with_fm.R`: Measures contemporaneous correlation between CSM portfolio returns and factor momentum returns.
	- Fixed small issues, done
- `3b_sym_spanning.R`: spanning regressions of CSM cumulative returns on FM
	- Fixed small issues, can mostly replicate, done
- `4a_by_liquidity_3x3.R`: Computes 1-month CSM returns in 3x3 source/target liquidity buckets using liquidity-split signal matrices.
	- I broke it into two. Too memory intensive
		- 4a1 just computes the combined version and saves into JD folder
		- 4a2 compute profits one by one
	- Q: where are the upstream ones calculated? In /JD folder?
	- In 4a2, the later part is just checking results, not outputting, right? 
- `4b_by_liquidity_3x3_scale.R`: (TO SKIP) Scales 3x3 liquidity-bucket returns to match baseline signal scaling conventions and saves standardized outputs.
	- QUESTION: this involved "scaling again". Why? 
- `4c_by_liquidity_3x3_produce_table.R`: (TOSKIP) Summarizes scaled 3x3 liquidity return panels into table-ready means and standard errors.
	- QUESTION: I don't think the results here are right. It is true that higher liquidity source stock means more profits, but there is no clear difference by target liquidity
- `5a_download_factors.R`: Downloads Fama-French factors into a local factor file. 
	- done. 
- `5b_other_methods_spanning.R`: Runs time-series spanning regressions of CSM returns on FF factor sets to test whether alpha survives controls.
	- done. 
- `5c_other_methods_fama_macbeth.R`: Estimates cross-sectional return predictability with Fama-MacBeth regressions under progressively richer controls.
	- done
- `6a_liquidity_half_half.R`: Main fast implementation for constructing liquidity-conditioned signal decomposition outputs from matrix data.
	- DID NOT rerun. I ran out of memory. 
	- The code really should be broken up into scripts? TODO
- `7_debug_cf_dr.R`: Sandbox script with replication/debug checks for ICC summary stats and CF/DR decomposition diagnostics.
- `7_diagnostic_cf_dr.R`: Produces diagnostic validation tables/plots for ICC and CF-vs-DR news decomposition behavior across horizons.
- `7a1_solve_ICC.R`: Solves implied cost of capital (ICC) from analyst data and saves paired current/next-period ICC inputs.
- `7a1_solve_ICC_t_t+h.R`: Solves ICC in a base-month framework suitable for dynamic `t` to `t+h` horizon pairing.
- `7a2_cf_dr_decomposition.R`: Performs CF/DR price-change decomposition using paired `t` and `t+1` ICC/cash-flow inputs and cross-term pricing.
- `7a2_cf_dr_decomposition_t_t+h.R`: Extends CF/DR decomposition to all horizons (`t` to `t+h`) and saves per-horizon decomposition files.
- `7b1_reversals_include_sym_asy_cf_dr.R`: Builds signal-sorted return panels where future outcomes are CF, DR, and total news components.
- `7b1_reversals_include_sym_asy_cf_dr_new.R`: Currently empty placeholder file for a revised CF/DR reversal implementation.
- `7b1_reversals_include_sym_asy_cf_dr_t_t+h.R`: Updated CF/DR reversal builder using precomputed multi-horizon (`1` to `180`) news decomposition inputs.
- `7c1_reversals_tstats_nw_include_sym_asy_cd_dr.R`: Computes Newey-West statistics for CF/DR-based long-short return decompositions across horizon windows.
- `7c1_reversals_tstats_nw_include_sym_asy_cd_dr_t_t+h.R`: Revised/Newey-West CF/DR summarizer aligned with cumulative `t` to `t+h` construction logic.
- `7d1_reversals_plot_sym_asy_cd_dr.R`: Creates plotting data and figures for symmetric/asymmetric CF/DR cumulative return trajectories.
- `7e1_Using_JC_Code.R`: Explores whether signal-sorted portfolios forecast cumulative LTG forecast changes using coauthor data conventions.
- `8_Detail_LTG.R`: Builds monthly detailed LTG change series from consensus data and compares signal-linked LTG dynamics.
- `8_Detail_LTG_Raw_Data.R`: Processes raw detailed LTG analyst/broker data into monthly forward-filled and winsorized LTG change panels.
- `8e1_Using_JC_Code_Input_Individual_LTG.R`: Re-runs LTG mechanism tests using individual-level detailed LTG change inputs.
- `8f_Compare_Detail_JD_JC.R`: Compares old vs detailed LTG change constructions and visualizes their cross-sectional correlation over time.
- `runmefirst.R`: Shared environment bootstrap script that loads packages, options, and common settings used by most scripts.


## `tables/`

- **combining_predictors** — combining 8 into 1 using a regression (TODO)
- **corr_with_fm** — each signal’s correlation with factor momentum.
	- done
- **fm_reg_combined** — LaTeX table of Fama–MacBeth regression output for the combined predictor (`tmp/other_methods/.../fm_results_summary.RDS`).
	- done
- **liq_illiq_combined** — LaTeX (and related Rmd) tables from liquidity-conditioned pairwise decomposition (`pairwise_decom_table.Rds`), mainly spread vs volume side-by-side; extra Rmds for bid–ask and dollar-volume variants.
	- done. Code.Rmd is the main file. The others are just for visualization via pdf (all not large)
- **reversal_decomp_nw** — Newey–West results for combined signals by decomposition (total / sym / asy). 
	- Done. 
- **reversal_nw** — Newey–West total across return horizons. 
	- Done
- **ts_reg_combined** — time-series spanning results. 
	- done


## `plots/`

- `1_reversal_with_fm.R`
- `1a_reversals_main.R`
- `1b_reversals_spanning_factor_mom.R'
- `1c_reversals_fm_controls.R`
- `1d_survival_rate.R`
- `2a_profit_fraction_of_sym.R`
- `4a_liquidity_target_stock_sort.R`
- `5a_mechanism_ltg.R`