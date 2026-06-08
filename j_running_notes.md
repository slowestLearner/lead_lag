## Current

My main feeling is we need to bring back decile results
	Modifying 2b1

Buy and hold return - will results be different? 
Results that show insensitivity to delisting return --- I guess we just put that aside for now


## Writing

Done with first two sections

1. Introduction `{sec:intro}`
  - Related literature `{sec:intro:related}`

2. Cross-Stock Momentum Reverses `{sec:reversal}`
  - Data and cross-stock momentum predictors `{sec:reversal:data}`
  - Computing cross-stock momentum predictors `{sec:reversal:computation}`
  - Cross-stock momentum reverses `{sec:reversal:portfolio}`
  - Robustness `{sec:reversal:robustness}`

3. Decomposing Cross-Stock Momentum `{sec:decomp}`
  - Common components revert: preliminary evidence `{sec:decomp:preliminary}`
  - CSM Decomposition `{sec:decomp:methodology}`
  - The common component accounts for the reversal `{sec:decomp:reversal}`
  
4. Interpreting Decomposed CSM Components `{sec:understanding}`
  - The symmetric component `{sec:understanding:sym}`
  - The asymmetric component: liquid stocks lead illiquid ones `{sec:understanding:asy}`

5. Relationship with factor and stock momentum `{sec:other_momentum}`

6. Conclusion `{sec:conclusion}`

-- Appendix

- A. Additional Empirical Results `{app:empirical}`
  - Delisting `{app:empirical:delisting}`
  - Bootstrapping standard errors `{app:empirical:bootstrap}`
  - CSM reversals: robustness `{app:empirical:robustness}`
  - Regression-based combination of CSM predictors `{app:empirical:combining}`
  - Statistical power in assessing long-horizon CSM portfolio returns `{app:empirical:power}`
  - Peer versus focal portfolios `{app:empirical:common}`

- B. CSM Decomposition `{app:decomp}`
  - Expressing CSM predictors using prediction matrices `{app:decomp:prediction_matrix}`
  - Additional details `{app:decomp:additional}`



# Top-Level R Script Summaries (`code_final`)


- `0_main_file.R`: Master runner script that sources the main preprocessing and reversal/statistics scripts in sequence.
- `1a_signal_availability.R`: Computes monthly equal- and value-weighted coverage rates of each signal across the stock universe.
	- done
- `1b_imputing_predictors.R`
	- new
- `2a1_get_residuals_of_csm_signals.R`: Residualizes total CSM signals on characteristics and industry controls, then saves cleaned signal panels.
	- have some differences from earlier signals due to different (i,j) universe requirements
- `2a2_get_residuals_of_csm_signals_super_set.R`: Residualizes CSM signals using a larger literature control set and saves the supersetted residual signals.
	- code can be merged into 2a1? Also, it seems that the “superset” list is already in the chen et al characteristics? (DO LATER)
- `2a3_Burt_Hrdlicka_Adjust.R`: Implements Burt-Hrdlicka style adjustments by first estimating factor residuals and then constructing adjusted signals.
	- NOTE: the first part of the code only runs the regression if factor data is fully available. Is this an issue? You can check "tmp/burt_hrdlicka/CRSP_BH.RDS" and compare with monthly stock returns. They are identical for the first half of the data (where liquidity factor is not present)
- `2b1_reversals_only_total_different_specs.R`: Builds long-run return paths for total-signal portfolios across multiple signal construction specifications.
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


### CF/DR (cash-flow vs discount-rate) reversal block (`7*`)

- `7a1_move_data.R`: Copies/stages raw inputs (GDP growth, monthly CRSP prices, FF12 industry def, Compustat annual, IBES eps/ltg/price) into `tmp/raw_data/`.
  done
- `7a2_get_ind_assignment.R`: Maps stocks to FF12 industries by SIC code and builds quarterly industry-assignment-and-market-cap panel.
  done
- `7a3_get_plowback_rate_by_industry.R`: Computes industry-level net-payout-to-earnings (payout) ratios from Compustat for use as the valuation payout assumption.
  done
- `7a4_get_industry_ltg.R`: Builds industry-level (EW and VW) average analyst long-term growth (LTG) used as the terminal growth anchor.
  done
- `7b1_chen_et_al_contemp.R`: Solves for each stock-quarter's implied cost of capital (q/ICC) via a Chen-et-al-style discounted-earnings model and saves contemporaneous valuation components.
  done
- `7b2_chen_et_al_lead_lag.R`: For each period, recomputes valuation using ICC from previous periods (lags 0-40q), isolating the cash-flow channel.
  done
- `7b3_implied_cf_return.R`: Converts lagged-ICC valuations into implied cash-flow-justified log returns from end-of-t to end-of-t+h.
  done
- `7b4_fill_cf_return_forward.R`: Forward-fills implied CF returns across horizons up to several staleness caps (1,2,4,8,20,40q) via repeated self-merges.
  done  
- `7b5_fill_raw_return_forward_jd_archive.R`: Builds true cumulative *raw* CRSP daily returns over the matching quarterly horizons as the realized-return benchmark.
  TO DELETE
- `7b5_fill_raw_return_forward.R` - implements the same but faster
  done
- `7c1_reversals_include_sym_asy_cf_dr_using_jl_crsp.R`: Forms sym/asy/total/combined signal portfolios and computes CF-return reversal paths over pre/post horizons.
  done
- `7c2_reversals_tstats_nw_include_sym_asy_cd_dr_t_t+h.R`: Computes Newey-West means/t-stats of cumulative CF/raw portfolio returns by horizon, with early/late combined subsamples.
  done
- `7c3_reversals_plot_sym_asy_cd_dr.R`: Plots the sym/asy CF-vs-DR reversal paths with NW confidence bands across fill-forward horizons.
  done. TODO - this should go to a plot folder later


### Misc numbers and autocorrelation (`9*`)

- `9a_various_numbers.R`: Computes one-off figures cited in text (e.g. fraction of stocks lacking delisting returns).
- `9b1_signal_autocorrelation.R`: Estimates one-month autocorrelation of CSM signals (per signal + combined) under several fixed-effects specifications.
- `9b2_autocorrelation_of_other_signals.R`: Runs the same autocorrelation regressions on the Chen characteristics set for comparison/benchmarking.


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

- `1_reversal_with_fm.R`: plot CSM return along with factor momentum
	- Not sure if we still need it
- `1a_reversals_main.R`: Produces the baseline reversal plots.
	- done
- `1a2_reversals_combined_split.R`: main CSM split into two time periods. 
- `1b_reversals_spanning_factor_mom.R`: Visualizes reversal performance after spanning adjustments on factor momentum.
- `1c_reversals_fm_controls.R`: Plots reversal paths from specifications that include Fama-MacBeth-style control sets.
- `1d_survival_rate.R`: survival rates. 
	- Done. 
- `2a_profit_fraction_of_sym.R`: Visualizes the symmetric/asymmetric share decomposition of combined reversal profits.
- `4a_liquidity_target_stock_sort.R`: Produces liquidity-target-sorted plots for source-target return spread patterns.
- `5a_mechanism_ltg.R`: Plots mechanism evidence linking signals to LTG dynamics and related outcome series.