# `tables/` subfolders

- **corr_with_fm** — LaTeX table of each signal’s correlation with the Fama–MacBeth object (total / sym / asy), with sample sizes and signal ordering.
- **fm_reg_combined** — LaTeX table of Fama–MacBeth regression output for the combined predictor (`tmp/other_methods/.../fm_results_summary.RDS`).
- **ts_reg_combined** — LaTeX table of time-series spanning results (combined signal vs required FF5 setup from `time_series_spanning`).
- **reversal_nw** — LaTeX table of Newey–West total-signal portfolio coefficients across return horizons, with stars and signal ordering.
- **reversal_decomp_nw** — LaTeX table of Newey–West results for combined signals by decomposition (total / sym / asy), plus symmetric vs asymmetric fraction rows from the NW-fraction summary RDS.
- **liq_illiq_combined** — LaTeX (and related Rmd) tables from liquidity-conditioned pairwise decomposition (`pairwise_decom_table.Rds`), mainly spread vs volume side-by-side; extra Rmds for bid–ask and dollar-volume variants.
- **liq_3x3_combined** — LaTeX table of 3×3 liquidity-sorted portfolio returns (combined signal, non-total `var_type`) from the summarized 3×3 RDS.
- **profit_by_liquidity** — LaTeX table of profit by liquidity deciles (ME sort, combined signal, decile portfolios) from the sym-mechanism / liquidity deciles RDS.
