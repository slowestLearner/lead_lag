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


# return data
rdata <- readRDS("../../data/Stocks/Monthly_CRSP.RDS")[, .(yyyymm, permno, primexch, ret, keep)] %>%
  mutate(idx = frank(yyyymm, ties.method = "dense"))
rdata_factors <- merge(factors, rdata[, .(yyyymm, permno, ret, idx)], by = c("yyyymm")) %>% setDT()
rm(factors)
factors_list <- c("mktrf", "smb", "hml", "umd", "liquidity")
reg_formula <- as.formula(paste("ret ~", paste(factors_list, collapse = " + ")))

windows_df <- rdata_factors[, .(
  permno,
  yyyymm,
  idx,
  start_idx = idx - 12, # The start of the look-back window
  end_idx = idx - 1 # The end of the look-back window
)]









# with 6 cores, takes around X mins
plan(multisession, workers = nc)
print(paste("Starting parallel processing on", nc, "cores."))

# split data into the same num of chunks as the num of cores
n_rows <- nrow(windows_df)
chunk_ids <- cut(seq_len(n_rows), breaks = nc, labels = FALSE)
window_chunks <- split(windows_df, chunk_ids)


tic()
list_of_results <- future_lapply(
  X = window_chunks,
  FUN = process_window_chunk,
  historical_data = rdata_factors,
  factors = factors_list,
  future.seed = 42
)
toc()
plan(sequential)

coef_est_parallel <- rbindlist(list_of_results)


coefs_to_merge <- coef_est_parallel %>%
  dplyr::select(permno, 4:ncol(.)) %>%
  dplyr::rename(alpha = "(Intercept)")

factors_list <- c("mktrf", "smb", "hml", "umd", "liquidity")
coef_names <- c("alpha", factors_list)
new_coef_names <- c("alpha_est", paste0(factors_list, "_beta"))


setnames(coefs_to_merge, old = coef_names, new = new_coef_names)
setnames(coefs_to_merge, "idx_current", "idx")

final_data <- rdata_factors[coefs_to_merge,
  on = .(permno, idx, yyyymm),
  nomatch = 0
] # Use nomatch=0 for an inner join

final_data[, fitted_return := alpha_est +
  mktrf_beta * mktrf +
  smb_beta * smb +
  hml_beta * hml +
  umd_beta * umd +
  liquidity_beta * liquidity]

final_data <- final_data[, .(
  permno,
  yyyymm,
  residual = ret - coalesce(fitted_return, 0)
)]

saveRDS(final_data, file = "tmp/burt_hrdlicka/CRSP_BH.RDS")

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

    plan(multisession, workers = 6)

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
