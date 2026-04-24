# --- adjust signals using the burt and hrdlicka method. First chunk computes factor residuals. Second chunk computes signals
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code
source("runmefirst.R")

# --- 1) part 1, compute return residuals

# load factor returns
factors <- read_sas("tmp/factors/factors_all.sas7bdat") %>% setDT()
factors[is.na(factors)] <- 0 # mom and liquidity is filled zero over earlier periods
factors[, yyyymm := 100 * year(date) + month(date)]
factors[, date := NULL]
factors <- factors[yyyymm <= 202412]
setorder(factors, yyyymm) # Ensure ordered by time

factors[, idx_global := .I] 
factors_list <- c("mktrf", "smb", "hml", "umd", "liquidity")
X_global <- as.matrix(cbind("(Intercept)" = 1, factors[, ..factors_list]))


# return data
rdata <- readRDS("../../data/Stocks/Monthly_CRSP.RDS")[, .(yyyymm, permno, ret)] %>%
  setDT()


time_map <- factors[, .(yyyymm, idx_global)]
rdata <- merge(rdata, time_map, by = "yyyymm")

setorder(rdata, permno, idx_global)
windows_df <- rdata[, .(
  yyyymm,
  idx_global,
  start_idx = idx_global - 12, 
  end_idx = idx_global - 1
), by = permno]

reg_formula <- as.formula(paste("ret ~", paste(factors_list, collapse = " + ")))

process_window_chunk <- function(window_chunk, historical_data, X_global, coef_names) {
  
  setDTthreads(1) # Crucial for parallel stability
  
  # Rolling Join
  # Note the use of `x.` prefix inside `j` to guarantee we get HISTORICAL data
  result_chunk <- historical_data[window_chunk,
                                  on = .(permno, idx_global >= start_idx, idx_global <= end_idx),
                                  by = .EACHI,
                                  j = {
                                    # Initialize Output
                                    output <- setNames(as.list(rep(NA_real_, length(coef_names))), coef_names)
                                    output$yyyymm <- i.yyyymm
                                    output$idx <- i.idx_global
                                    
                                    # SAFE DATA EXTRACTION
                                    # Use 'x.' prefix to explicitly grab columns from 'historical_data'
                                    # .SD cols in non-equi join can be tricky, referencing x.Var is safer
                                    ret_vec <- x.ret
                                    idx_vec <- x.idx_global
                                    
                                    # Validation
                                    # Check 1: Return must be finite
                                    valid_y <- is.finite(ret_vec)
                                    # Check 2: Index must be valid (not NA)
                                    valid_idx <- !is.na(idx_vec)
                                    
                                    keep_rows <- valid_y & valid_idx
                                    
                                    if (sum(keep_rows) >= 8) {
                                      y <- ret_vec[keep_rows]
                                      curr_indices <- idx_vec[keep_rows]
                                      
                                      # Subset Global Matrix
                                      X <- X_global[curr_indices, , drop = FALSE]
                                      
                                      # Check for Bad Rows in X (e.g. index mismatch returning NAs)
                                      bad_x <- rowSums(!is.finite(X)) > 0
                                      
                                      if (any(bad_x)) {
                                        X <- X[!bad_x, , drop = FALSE]
                                        y <- y[!bad_x]
                                      }
                                      
                                      if (length(y) >= 8) {
                                        # Identify Valid Columns (handle zero-filled factors)
                                        valid_cols <- colSums(abs(X), na.rm = TRUE) > 1e-9
                                        valid_cols[1] <- TRUE # Always keep Intercept
                                        
                                        # Run Regression
                                        # tryCatch protects against singular fits crashing the worker
                                        fit <- tryCatch(lm.fit(X[, valid_cols, drop=FALSE], y), error=function(e) NULL)
                                        
                                        if (!is.null(fit)) {
                                          estimated_coefs <- fit$coefficients
                                          output[names(estimated_coefs)] <- estimated_coefs
                                        }
                                      }
                                    }
                                    
                                    output
                                  },
                                  nomatch = 0
  ]
  
  return(result_chunk)
}





all_coef_names <- c("(Intercept)", factors_list)

# with 6 cores, takes around X mins
plan(multisession, workers = nc)
tic(paste("Starting parallel processing on", nc, "cores."))

# split data into the same num of chunks as the num of cores
n_rows <- nrow(windows_df)
chunk_ids <- cut(seq_len(n_rows), breaks = nc, labels = FALSE)
window_chunks <- split(windows_df, chunk_ids)



list_of_results <- future_lapply(
  X = window_chunks,
  FUN = process_window_chunk,
  historical_data = rdata,
  X_global = X_global,
  coef_names = all_coef_names,
  future.seed = 42
)
plan(sequential)

toc()
coef_est_parallel <- rbindlist(list_of_results)


coefs_to_merge <- coef_est_parallel %>%
  dplyr::select(permno, 4:ncol(.)) %>%
  dplyr::rename(alpha = "(Intercept)")

factors_list <- c("mktrf", "smb", "hml", "umd", "liquidity")
coef_names <- c("alpha", factors_list)
new_coef_names <- c("alpha_est", paste0(factors_list, "_beta"))


setnames(coefs_to_merge, old = coef_names, new = new_coef_names)


rdata_factors <- merge(factors[, .(mktrf, smb, hml, umd, liquidity, yyyymm, idx = idx_global)],
                       rdata[, .(yyyymm, permno, ret, idx = idx_global)], 
                       by = c("yyyymm", "idx")) %>% setDT()

final_data <- rdata_factors[coefs_to_merge,
  on = .(permno, idx, yyyymm),
  nomatch = 0
] # Use nomatch=0 for an inner join

setorder(final_data, yyyymm, permno)
final_data[, fitted_return := coalesce(alpha_est, 0) +
             coalesce(mktrf_beta, 0) * mktrf +
             coalesce(smb_beta, 0) * smb +
             coalesce(hml_beta, 0) * hml +
             coalesce(umd_beta, 0) * umd +
             coalesce(liquidity_beta, 0) * liquidity]

final_data <- final_data[, .(
  permno,
  yyyymm,
  ret = ret,
  residual = ret - coalesce(fitted_return, 0)
)]
# corr <- final_data %>%
#   dplyr::group_by(yyyymm) %>%
#   summarise(cor_coef = cor(ret, residual, use = "pairwise.complete.obs")) %>%
#   ungroup() %>%
#   mutate(date = make_date(yyyymm %/% 100, yyyymm %% 100, 1))
# 
# ggplot(corr, aes(x = date, y = cor_coef)) +
#   # 1. Add Line and Points
#   geom_line(color = "steelblue", linewidth = 0.1) +
#   theme_bw()


saveRDS(final_data[, ret := NULL], file = "tmp/burt_hrdlicka/CRSP_BH.RDS")

# --- part 2), compute signals using BH residuals
source("runmefirst.R")
library(lubridate)

# residualized returns
msf_i <- readRDS(file = "tmp/burt_hrdlicka/CRSP_BH.RDS") %>%
  dplyr::rename(ret = residual)


date_df <- msf_i %>%
  distinct(yyyymm) %>%
  dplyr::mutate(date = ceiling_date(as_date(ISOdate(str_sub(yyyymm, 1, 4),
    str_sub(yyyymm, 5, 6),
    day = 01
  )) %m+% months(0), "month") - days(1)) %>%
  data.table()


for (stock_base in c("all", "large")) {
  to_dir <- file.path("tmp/burt_hrdlicka/", stock_base, "csmom/")
  library(fs)
  dir_create(to_dir, recursive = T)



  crossmom_vars <- c("Analyst", "BEAcustomer", "BEAsupplier", "Econ", "Geo", "Indu", "Pseudo", "Tec")

  for (crossmom_i in crossmom_vars) {
    print(paste("Processing:", crossmom_i))

    matrix_path <- file.path("../../../Leadlag/data/Predictionandsignal", crossmom_i, stock_base, "matrix")
    permno_path <- file.path("../../../Leadlag/data/Predictionandsignal", crossmom_i, stock_base, "permno")

    datalist <- dir(
      path = matrix_path,
      pattern = "*.Rds", full.names = TRUE
    )

    valid_dates <- paste0(unique(date_df$date), ".Rds")
    datalist <- datalist[basename(datalist) %in% valid_dates]

    plan(multisession, workers = 4)

    # file_path <- datalist[1]
    df_save <- future_lapply(datalist, function(file_path) {
      date_filter <- parse_date(str_sub(basename(file_path), 1, -5))
      yyyymm_t <- year(date_filter) * 100 + month(date_filter)

      pi_matrix <- readRDS(file = file_path)

      # 1. Center the raw matrix - NEW
      pi_matrix <- scale(pi_matrix, center = TRUE, scale = FALSE)


      permno_file <- file.path(permno_path, basename(file_path)) # this file tells us which permnos are included by each position in pi_matrix
      col_df <- readRDS(file = permno_file) %>%
        dplyr::arrange(permno) %>%
        data.table()



      # Filter msf_i for the current month
      msf_monthly <- msf_i[yyyymm == yyyymm_t]

      monthly_data <- merge(col_df, msf_monthly, by = "permno", all.x = TRUE)
      setorderv(monthly_data, "permno") # Ensure order is correct


      monthly_data[, ret_demeaned := ret - mean(ret, na.rm = TRUE)]


      monthly_data[is.na(ret_demeaned), ret_demeaned := 0]

      setorderv(monthly_data, "permno")

      returns_vec <- monthly_data$ret_demeaned


      signal_df <- data.table(
        permno = col_df$permno,
        yyyymm = yyyymm_t,
        signal_bh = as.vector(pi_matrix %*% returns_vec)
      )
      return(signal_df)
    },
    # future.packages = c("lubridate"),
    future.seed = 42
    )


    plan(sequential)


    final_df <- rbindlist(df_save)
    saveRDS(final_df, file = paste0(to_dir, "/", crossmom_i, ".Rds"))


    rm(df_save, final_df)
    gc()
  }

  library(stringr)

  datalist <- dir(
    path = to_dir,
    pattern = paste0(".Rds")
  )

  file_map <- c(
    "Analyst.Rds"     = "analyst",
    "BEAcustomer.Rds" = "beacustomer",
    "BEAsupplier.Rds" = "beasupplier",
    "Econ.Rds"        = "econ",
    "Geo.Rds"         = "geo",
    "Indu.Rds"        = "industry",
    "Pseudo.Rds"      = "pseudo",
    "Tec.Rds"         = "tech"
  )

  df_all <- bind_rows(lapply(datalist, function(i) {
    dt <- readRDS(file.path(to_dir, i))
    setDT(dt)
    dt[, var := file_map[i]]

    return(dt)
  })) %>%
    data.table()






  # Generate the combined signal by average across var
  cols_to_average <- c("signal_bh")

  vars_per_month <- df_all[, .(N = uniqueN(var)), by = yyyymm]

  df_all[vars_per_month, on = "yyyymm", N_denom := i.N]


  df_combined <- df_all[, lapply(.SD, function(x) sum(x, na.rm = TRUE) / N_denom[1]),
    by = .(yyyymm, permno),
    .SDcols = cols_to_average
  ]

  df_combined[, var := "combined"]

  df_all[, N_denom := NULL]

  # df_combined <- df_all[, lapply(.SD, mean, na.rm = TRUE),
  #                            by = .(yyyymm, permno),
  #                            .SDcols = cols_to_average][, var := "combined"]




  to_dir <- file.path("tmp/raw_data/signals/", stock_base, "total_signal/")
  dir.create(to_dir, recursive = T)
  saveRDS(bind_rows(df_combined, df_all), paste0(to_dir, "/signals_bh.RDS"))
}

# # --- SANITY
# source("runmefirst.R")

# data <- readRDS("tmp/burt_hrdlicka_new/CRSP_BH.RDS")
# tmp <- readRDS("../../data/Stocks/Monthly_CRSP.RDS") %>%
#   select(yyyymm, permno, ret) %>%
#   na.omit() %>%
#   setDT()

# data <- data[tmp, on = c("yyyymm", "permno"), nomatch = 0]
# plot(data[, cor(ret, residual), yyyymm])




# process_window_chunk <- function(window_chunk, historical_data, factors) {
#   all_coef_names <- c("(Intercept)", factors)
# 
#   result_chunk <- historical_data[window_chunk,
#     on = .(permno == permno, idx >= start_idx, idx <= end_idx),
#     by = .EACHI,
#     j = {
#       output <- setNames(as.list(rep(NA_real_, length(all_coef_names))), all_coef_names)
#       complete_data <- .SD[complete.cases(.SD[, c("ret", ..factors)])]
#       if (nrow(complete_data) >= 8) {
#         y <- complete_data$ret
#         X <- as.matrix(cbind("(Intercept)" = 1, complete_data[, ..factors]))
#         if (qr(X)$rank == ncol(X)) {
#           estimated_coefs <- lm.fit(X, y)$coefficients
#           output[names(estimated_coefs)] <- estimated_coefs
#         }
#       }
#       output$yyyymm <- i.yyyymm
#       output$idx_current <- i.idx
#       output
#     },
#     nomatch = 0
#   ]
#   return(result_chunk)
# }














