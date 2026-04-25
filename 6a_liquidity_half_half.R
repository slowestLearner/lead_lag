# --- Main fast implementation for constructing liquidity-conditioned signal decomposition outputs from matrix data
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

# loop through stock types
for (stock_base in c("all", "large")) {
  # stock_base <- "all"

  if (.Platform$OS.type == "windows") {
    base_dir <- "D:/Dropbox/Leadlag/formal_tests/tmp/asy/measuring_arb_cost/fitted_measures/"
  } else {
    base_dir <- "~/Dropbox/SpeculativeIdeas/Leadlag/formal_tests/tmp/asy/measuring_arb_cost/fitted_measures/"
  }

  # liq measures
  liq_measure <- readRDS(paste0(base_dir, "put_together.RDS")) %>%
    mutate(
      minus_spread = -exp(logspreadhat),
      turnhat = exp(logturnhat)
    ) %>%
    transmute(yyyymm, permno, minus_spread, volume = turnhat * me_1) %>% # minus_spread, the larger, the more liquid
    na.omit()

  # get dates
  date_df <- liq_measure %>%
    dplyr::distinct(yyyymm) %>%
    dplyr::mutate(date = ceiling_date(as_date(ISOdate(str_sub(yyyymm, 1, 4),
      str_sub(yyyymm, 5, 6),
      day = 01
    )) %m-% months(0), "month") - days(1))

  # join onto monthly returns
  msf <- readRDS(file = "../../data/Stocks/Monthly_CRSP.RDS") %>%
    dplyr::select(yyyymm, permno, ret, keep) %>%
    dplyr::left_join(liq_measure, by = c("yyyymm", "permno")) %>%
    dplyr::group_by(yyyymm) %>%
    dplyr::filter(!all(is.na(minus_spread) | is.na(volume))) %>%
    dplyr::ungroup() %>%
    setDT()



  # helper function to process a single file
  process_single_file <- function(file_name, matrix_path, permno_path, msf, crossmom_i, to_dir) {
    # 1. Setup and Check Existence
    setDTthreads(1)

    date_filter <- parse_date_time(str_sub(file_name, 1, -5), orders = c("ymd", "ydm", "mdy"))
    yyyymm_i <- year(date_filter) * 100 + month(date_filter)
    file_tosave <- file.path(to_dir, paste0(yyyymm_i, ".Rds"))

    if (file.exists(file_tosave)) {
      return(NULL)
    } # Fail fast


    col_dt <- readRDS(file.path(permno_path, file_name))
    if (is.matrix(col_dt) || is.vector(col_dt)) col_dt <- as.data.table(col_dt) # Handle varied inputs
    if (!"permno" %in% names(col_dt)) setnames(col_dt, 1, "permno") # Ensure col name

    # Ensure sorted and distinct for indexing
    permnos_vec <- sort(unique(col_dt$permno))
    n_stocks <- length(permnos_vec)

    # Create a lookup table for fast alignment
    idx_dt <- data.table(permno = permnos_vec, idx = 1:n_stocks)


    pi <- readRDS(file.path(matrix_path, file_name))


    if (nrow(pi) != n_stocks) stop(paste("Dimension mismatch in", file_name))
    pi_t <- t(pi)
    pi_s <- (pi + pi_t) / 2
    pi_a <- (pi - pi_t) / 2
    rm(pi)

    # 4. Create the Lean Long Table (Integer Indices only)
    idx_seq <- 1:n_stocks

    dt_base <- data.table(
      i = rep(idx_seq, each = n_stocks), # Row Index
      j = rep(idx_seq, times = n_stocks), # Col Index
      total = as.vector(pi_t),
      asy = as.vector(t(pi_a)),
      sym = as.vector(t(pi_s))
    )
    rm(pi_t, pi_s, pi_a) # Free memory

    # 5. Prepare T+1 Data (Target)
    date_filter_1 <- ceiling_date(date_filter %m+% months(1), "month") - days(1)
    yyyymm_i1 <- year(date_filter_1) * 100 + month(date_filter_1)

    # Get T+1 Returns and map to index 1..N
    dt_t1 <- msf[yyyymm == yyyymm_i1, .(permno, ret_t1 = ret)]
    dt_t1 <- idx_dt[dt_t1, on = "permno", nomatch = NULL] # Inner join to keep only relevant


    ret_t1_vec <- numeric(n_stocks) * NA
    ret_t1_vec[dt_t1$idx] <- dt_t1$ret_t1


    dt_base[, ret_t1_val := ret_t1_vec[i]]


    dt_base <- dt_base[!is.na(ret_t1_val)]

    rm(dt_t1, ret_t1_vec)
    gc()

    # 6. Loop over Liquidity Measures
    liq_measures <- c("minus_spread", "volume")
    master_results <- vector("list", length(liq_measures) * 2 * 3) # Pre-allocate list
    m_idx <- 1

    # Prepare T data wrapper
    msf_current <- msf[yyyymm == yyyymm_i]

    for (liq_var in liq_measures) {
      curr_data <- msf_current[, .(permno, ret_t = ret, keep = keep, spread = get(liq_var))]

      # Map to 1..N indices
      curr_data <- idx_dt[curr_data, on = "permno", nomatch = NULL]

      vec_ret_t <- rep(NA_real_, n_stocks)
      vec_spread <- rep(NA_real_, n_stocks)
      vec_keep <- rep(NA_real_, n_stocks)

      vec_ret_t[curr_data$idx] <- curr_data$ret_t
      vec_spread[curr_data$idx] <- curr_data$spread
      vec_keep[curr_data$idx] <- curr_data$keep

      # Handle NA Spreads (Mean imputation)
      avg_spread <- mean(vec_spread, na.rm = TRUE)
      if (is.nan(avg_spread)) avg_spread <- 0
      vec_spread[is.na(vec_spread)] <- avg_spread

      # --- B. Update Big Table (By Reference) ---

      dt_base[, `:=`(
        w_ret = ret_t1_val * vec_ret_t[j], # ret(t+1, i) * ret(t, j)
        sp_i  = vec_spread[i],
        sp_j  = vec_spread[j],
        kp_i  = vec_keep[i],
        kp_j  = vec_keep[j]
      )]

      # --- C. Calculate Spread Groups (Integer Math) ---
      # 1: p1 (j < i), 2: p2 (equal), 3: p3 (j > i)
      dt_base[, grp := fcase(
        sp_j < sp_i,  1L,
        sp_j == sp_i, 2L,
        sp_j > sp_i,  3L
      )]



      for (micro_val in c("yes", "no")) {
        threshold <- if (micro_val == "yes") 0 else 1

        valid_rows <- dt_base[kp_i >= threshold & kp_j >= threshold, which = TRUE]

        if (length(valid_rows) == 0) next


        for (part_name in c("total", "asy", "sym")) {
          # 1. Main Return Calculation

          agg_res <- dt_base[valid_rows, .(
            val = sum(get(part_name) * w_ret, na.rm = TRUE)
          ), by = grp]

          val_all <- dt_base[valid_rows, sum(get(part_name) * w_ret, na.rm = TRUE)]

          # Extract safely
          v_p1 <- agg_res[grp == 1, val]
          if (length(v_p1) == 0) v_p1 <- 0
          v_p2 <- agg_res[grp == 2, val]
          if (length(v_p2) == 0) v_p2 <- 0
          v_p3 <- agg_res[grp == 3, val]
          if (length(v_p3) == 0) v_p3 <- 0

          # 2. Leverage Calculation

          # This is the heaviest part remaining.
          lev_dt <- dt_base[valid_rows, .(
            s = sum(get(part_name) * vec_ret_t[j], na.rm = TRUE)
          ), by = .(i, grp)]

          # Total leverage (sum of abs across all i)

          lev_all_ungrouped <- dt_base[valid_rows, .(
            s = sum(get(part_name) * vec_ret_t[j], na.rm = TRUE)
          ), by = i]
          val_all_lev <- sum(abs(lev_all_ungrouped$s), na.rm = TRUE)

          # Grouped leverage
          final_lev <- lev_dt[, .(final = sum(abs(s), na.rm = TRUE)), by = grp]

          l_p1 <- final_lev[grp == 1, final]
          if (length(l_p1) == 0) l_p1 <- 0
          l_p2 <- final_lev[grp == 2, final]
          if (length(l_p2) == 0) l_p2 <- 0
          l_p3 <- final_lev[grp == 3, final]
          if (length(l_p3) == 0) l_p3 <- 0


          res_list <- list(
            data.table(
              all = val_all, p1 = v_p1, p2 = v_p2, p3 = v_p3,
              name = "unlev", part = part_name, date = date_filter_1,
              var = crossmom_i, micro = micro_val, liq_measure = liq_var
            ),
            data.table(
              all = if (val_all_lev == 0) 0 else val_all / val_all_lev,
              p1 = if (l_p1 == 0) 0 else v_p1 / l_p1,
              p2 = if (l_p2 == 0) 0 else v_p2 / l_p2,
              p3 = if (l_p3 == 0) 0 else v_p3 / l_p3,
              name = "lev", part = part_name, date = date_filter_1,
              var = crossmom_i, micro = micro_val, liq_measure = liq_var
            )
          )

          master_results[[m_idx]] <- rbindlist(res_list)
          m_idx <- m_idx + 1
        }
      }
    }

    # Final Cleanup
    rm(dt_base)
    out <- rbindlist(master_results)

    saveRDS(out, file.path(to_dir, paste0(yyyymm_i, ".Rds")))

    gc()
    return(NULL)
  }


  output_dir <- paste0("tmp/covliquidity/", stock_base, "/result/")

  cross_vars <- c(
    "BEAcustomer", "BEAsupplier",
    "Econ", "Geo", "Indu", "Pseudo", "Tec", "Analyst"
  )

  expected_files <- paste0(output_dir, cross_vars, ".Rds")
  cross_vars <- cross_vars[!file.exists(expected_files)]

  base_path <- "../../../Leadlag/data/Predictionandsignal"
  output_base <- paste0("tmp/covliquidity/", stock_base, "/data/")
  for (crossmom_i in cross_vars) {
    message(paste("Processing:", crossmom_i))

    matrix_path <- file.path(base_path, crossmom_i, stock_base, "matrix")
    permno_path <- file.path(base_path, crossmom_i, stock_base, "permno")


    all_files <- dir(path = matrix_path, pattern = "*.Rds")
    if (exists("date_df")) {
      target_files <- paste0(unique(date_df$date), ".Rds")
      datalist <- intersect(all_files, target_files)
    } else {
      datalist <- all_files
    }

    if (length(datalist) == 0) {
      message("No files found to process.")
      next
    }

    to_dir <- file.path(output_base)
    dir.create(to_dir, recursive = TRUE, showWarnings = FALSE)

    tic("Batch Execution")
    nc_used <- ifelse(crossmom_i == "Indu", 2, detectCores() - 2)

    plan(multisession, workers = nc_used)
    future_lapply(datalist, function(f) {
      process_single_file(f, matrix_path, permno_path, msf, crossmom_i, to_dir)
    },
    future.seed = TRUE,
    future.scheduling = 5
    )



    toc()
    gc()
    plan(sequential)
    datalist <- dir(
      path = to_dir,
      pattern = "*.Rds"
    )
    df_all <- bind_rows(lapply(datalist, function(i) {
      gg <- readRDS(file.path(to_dir, i))
    })) %>%
      data.table()


    for (col in names(df_all)) {
      if (is.numeric(df_all[[col]])) {
        set(df_all, which(is.nan(df_all[[col]])), col, 0)
      }
    }
    dir_create(paste0("tmp/covliquidity/", stock_base, "/result/"), recursive = TRUE)
    saveRDS(df_all, file = paste0("tmp/covliquidity/", stock_base, "/result/", crossmom_i, ".Rds"))
    unlink(file.path(to_dir, "*.Rds"))
  }






  to_dir <- paste0("tmp/covliquidity/", stock_base, "/result/")
  datalist <- dir(
    path = to_dir,
    pattern = ".Rds"
  )
  library(tidyr)
  data_all <- bind_rows(lapply(datalist, function(i) {
    gg <- readRDS(paste0(to_dir, i))
  })) %>%
    pivot_longer(!c(liq_measure, micro, var, part, date, name), names_to = "decom", values_to = "estimate")





  data_combined <- data_all %>%
    dplyr::group_by(liq_measure, micro, part, decom, date, name) %>%
    dplyr::summarise(estimate = mean(estimate, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(var = "combined")



  df <- bind_rows(data_all, data_combined)


  df_diff <- df %>%
    filter(decom == "p1") %>%
    dplyr::select(-decom) %>%
    dplyr::left_join(df %>%
      filter(decom == "p3") %>%
      dplyr::select(-decom), by = c(
      "liq_measure", "name", "part", "date",
      "var", "micro"
    )) %>%
    dplyr::mutate(
      estimate = estimate.y - estimate.x,
      decom = "p4"
    ) %>%
    dplyr::select(-estimate.x, -estimate.y)

  df_all <- bind_rows(df, df_diff) %>%
    data.table()

  saveRDS(df_all, file = paste0("tmp/covliquidity/", stock_base, "/pairwise_decom.Rds"))

  #
  fit.0 <- function(x) {
    m <- lm(estimate ~ 1, x)
    v <- vcovHC(m, type = "HC0")
    ct <- coeftest(m, vcov = v)[1, ]
    out <- as.list(ct)
    names(out) <- dimnames(ct)[[2]]
    out
  }
  library(lmtest)

  df_all <- readRDS(file = paste0("tmp/covliquidity/", stock_base, "/pairwise_decom.Rds"))

  port_decile <- df_all[, fit.0(.SD), by = list(liq_measure, micro, var, part, decom, name)] %>%
    dplyr::select(liq_measure, micro, name, var, part, decom, estimate = "V1", t = "V3") %>%
    dplyr::arrange(liq_measure, micro, name, var, part, decom) %>%
    dplyr::group_by(liq_measure, micro, name, var, part) %>%
    dplyr::mutate(estimate_all = estimate[1]) %>%
    dplyr::mutate(per = estimate / estimate_all) %>%
    dplyr::ungroup() %>%
    dplyr::select(-estimate_all) %>%
    dplyr::mutate(per = paste0("$", sprintf("%.2f", round(per * 100, 2)), "\\%$")) %>%
    dplyr::mutate(estimate = paste0(sprintf("%.3f", round(estimate, 3)))) %>%
    dplyr::mutate(t = paste0("(", sprintf("%.2f", round(t, 2)), ")")) %>%
    dplyr::arrange(liq_measure, micro, name, var, part, decom)

  data_alpha <- port_decile %>%
    dplyr::select(liq_measure, micro, name, var, part, decom, estimate) %>%
    pivot_wider(names_from = decom, values_from = estimate) %>%
    dplyr::mutate(parameter = "alpha") %>%
    dplyr::arrange(liq_measure, micro, name, var, part)

  data_t <- port_decile %>%
    dplyr::select(liq_measure, micro, name, var, part, decom, t) %>%
    pivot_wider(names_from = decom, values_from = t) %>%
    dplyr::mutate(parameter = "t") %>%
    dplyr::arrange(liq_measure, micro, name, var, part)

  data_per <- port_decile %>%
    dplyr::select(liq_measure, micro, name, var, part, decom, per) %>%
    pivot_wider(names_from = decom, values_from = per) %>%
    dplyr::mutate(parameter = "t_per") %>%
    dplyr::arrange(liq_measure, micro, name, var, part)


  data_table <- bind_rows(data_alpha, data_t, data_per)

  saveRDS(data_table, file = paste0("tmp/covliquidity/", stock_base, "/pairwise_decom_table.Rds"))
}
