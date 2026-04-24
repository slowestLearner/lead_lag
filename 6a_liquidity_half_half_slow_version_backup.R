rm(list = ls())
library(this.path)
library(lubridate)
library(stringr)
library(fs)
library(future.apply)
setwd(this.path::this.dir())
source("runmefirst.R")


for (stock_base in c("all", "large")){
  
  if (.Platform$OS.type == "windows") {
    base_dir <- "D:/Dropbox/Leadlag/formal_tests/tmp/asy/measuring_arb_cost/fitted_measures/"
  } else {
    base_dir <- "~/Dropbox/Leadlag/formal_tests/tmp/asy/measuring_arb_cost/fitted_measures/"
  }
  
  liq_measure <- readRDS(paste0(base_dir, "put_together.RDS")) %>%
    mutate(minus_spread = -exp(logspreadhat),
           turnhat = exp(logturnhat)) %>%
    transmute(yyyymm, permno, minus_spread, volume = turnhat * me_1) %>% #minus_spread, the larger, the more liquid
    na.omit()
  
  
  
  date_df <- liq_measure %>%
    dplyr::distinct(yyyymm) %>%
    dplyr::mutate(date = ceiling_date(as_date(ISOdate(str_sub(yyyymm, 1, 4), 
                                                      str_sub(yyyymm, 5, 6), day = 01)) %m-% months(0), "month") - days(1))
  
  msf <- readRDS(file = "../../data/Stocks/Monthly_CRSP.RDS") %>%
    dplyr::select(yyyymm, permno, ret, keep) %>%
    dplyr::left_join(liq_measure, by = c("yyyymm", "permno")) %>%
    dplyr::group_by(yyyymm) %>%
    dplyr::filter(!all(is.na(minus_spread) | is.na(volume))) %>%
    dplyr::ungroup()
  
  setDT(msf)
  
  
  
  
  
  process_single_file <- function(file_name, matrix_path, permno_path, msf, crossmom_i, to_dir) {
    
    
    
    setDTthreads(1)
    date_filter <- parse_date_time(str_sub(file_name, 1, -5), orders = c("ymd", "ydm", "mdy"))
    yyyymm_i <- year(date_filter) * 100 + month(date_filter)
    file_tosave <- file.path(to_dir, paste0(yyyymm_i, ".Rds"))
    
    if (file.exists(file_tosave)) {
      return(paste0("Skipped (Exists): ", yyyymm_i))
    }
    
    
    # Future Date (t+1)
    date_filter_1 <- ceiling_date(date_filter %m+% months(1), "month") - days(1)
    yyyymm_i1 <- year(date_filter_1) * 100 + month(date_filter_1)
    
    msf_t1 <- msf[yyyymm == yyyymm_i1, .(permno, ret_t1 = ret)]
    
    
    
    pi <- readRDS(file.path(matrix_path, file_name))
    col_dt <- readRDS(file.path(permno_path, file_name)) %>%
      data.table()
    
    col_dt[, DATE := NULL]
    
    setDT(col_dt)
    setorder(col_dt, permno) 
    
    permnos <- col_dt$permno
    n_stocks <- length(permnos)
    
    
    pi_t <- t(pi)
    pi_s <- (pi + pi_t) / 2
    pi_a <- (pi - pi_t) / 2
    rm(pi)
    vec_i <- rep(permnos, each = n_stocks)
    vec_j <- rep(permnos, times = n_stocks)
    
    dt_base <- data.table(
      permno_i = vec_i,
      permno_j = vec_j,
      total = as.vector(pi_t), # Transposed to match user sorting logic
      asy   = as.vector(t(pi_a)),
      sym   = as.vector(t(pi_s))
    )
    rm(pi_t, pi_s, pi_a, vec_i, vec_j)
    
    dt_base[msf_t1, on = .(permno_i = permno), ret_t1 := i.ret_t1]
    rm(msf_t1);gc()
    liq_measures <- c("minus_spread", "volume") # Add others here if needed
    master_results <- list()
    master_idx <- 1
    msf_current_month <- msf[yyyymm == yyyymm_i]
    for (liq_var in liq_measures) {
      msf_t <- msf_current_month [, 
                                  .(permno, 
                                    ret_t = ret, 
                                    keep_i = keep, 
                                    spread_i = get(liq_var))] 
      msf_t <- msf_t[col_dt, on = .(permno = permno)]
      
      avg_spread <- mean(msf_t$spread_i, na.rm = TRUE)
      if(is.nan(avg_spread)) avg_spread <- 0 # Safety check
      msf_t[is.na(spread_i), spread_i := avg_spread]
      
      dt_base[msf_t, on = .(permno_i = permno), `:=`(spread_i = i.spread_i, keep_i = i.keep_i)]
      dt_base[msf_t, on = .(permno_j = permno), `:=`(ret_t = i.ret_t, spread_j = i.spread_i, keep_j = i.keep_i)]
      
      rm(msf_t)
      
      
      dt_base[, ret_comb := ret_t * ret_t1]
      
      #Use INTEGERS for groups (1=p1, 2=p2, 3=p3)
      dt_base[, spread_group := fcase(
        spread_j < spread_i, 1L, 
        spread_j == spread_i, 2L,
        spread_j > spread_i, 3L 
      )]
      
      if (nrow(dt_base) == 0) next
      
      for (micro_val in c("yes", "no")) {
        
        threshold <- if(micro_val == "yes") 0 else 1
        
        valid_rows <- dt_base[keep_i >= threshold & keep_j >= threshold, which = TRUE]
        #sub_dt <- dt_base[keep_i >= threshold & keep_j >= threshold]
        
        if (length(valid_rows) == 0) next
        
        for (part_name in c("total", "asy", "sym")) {
          
          weight_vec <- dt_base[valid_rows, get(part_name) * ret_comb]
          val_all <- sum(weight_vec, na.rm = TRUE)
          
          agg_ret <- dt_base[valid_rows, .(val = sum(get(part_name) * ret_comb, na.rm = TRUE)), 
                             by = spread_group]
          
          # Extract values
          val_p1 <- agg_ret[spread_group == 1L, val]; if(length(val_p1)==0) val_p1 <- 0
          val_p2 <- agg_ret[spread_group == 2L, val]; if(length(val_p2)==0) val_p2 <- 0
          val_p3 <- agg_ret[spread_group == 3L, val]; if(length(val_p3)==0) val_p3 <- 0
          
          # Leverage Calculations
          lev_all_dt <- dt_base[valid_rows, .(s = sum(get(part_name) * ret_t, na.rm = TRUE)), by = permno_i]
          val_all_lev <- sum(abs(lev_all_dt$s), na.rm = TRUE)
          
          lev_grp_dt <- dt_base[valid_rows, .(s = sum(get(part_name) * ret_t, na.rm = TRUE)), 
                                by = .(permno_i, spread_group)]
          
          final_lev_grp <- lev_grp_dt[, .(final = sum(abs(s), na.rm = TRUE)), by = spread_group]
          
          val_p1_lev <- final_lev_grp[spread_group == 1L, final]; if(length(val_p1_lev)==0) val_p1_lev <- 0
          val_p2_lev <- final_lev_grp[spread_group == 2L, final]; if(length(val_p2_lev)==0) val_p2_lev <- 0
          val_p3_lev <- final_lev_grp[spread_group == 3L, final]; if(length(val_p3_lev)==0) val_p3_lev <- 0
          
          
          row_ret <- data.table(
            all = val_all, p1 = val_p1, p2 = val_p2, p3 = val_p3,
            name = "unlev", part = part_name, date = date_filter_1, 
            var = crossmom_i, micro = micro_val,
            liq_measure = liq_var  
          )
          
          row_lev <- data.table(
            all = if(val_all_lev==0) 0 else val_all/val_all_lev, 
            p1 = if(val_p1_lev==0) 0 else val_p1/val_p1_lev, 
            p2 = if(val_p2_lev==0) 0 else val_p2/val_p2_lev, 
            p3 = if(val_p3_lev==0) 0 else val_p3/val_p3_lev,
            name = "lev", part = part_name, date = date_filter_1, 
            var = crossmom_i, micro = micro_val,
            liq_measure = liq_var 
          )
          
          master_results[[master_idx]] <- rbind(row_ret, row_lev)
          master_idx <- master_idx + 1
        }
      }
      cols_to_remove <- c("spread_i", "spread_j", "keep_i", "keep_j", 
                          "ret_t", "spread_group")
      dt_base[, (cols_to_remove) := NULL]
    } # End liq_measures loop
    
    rm(dt_base) # Cleanup big object
    out <- rbindlist(master_results)
    
    saveRDS(out, file.path(to_dir, paste0(yyyymm_i, ".Rds")))
    
    gc()
    return(NULL) 
  }
  
  
  
  output_dir <- paste0("tmp/covliquidity/", stock_base, "/result/")
  
  cross_vars <- c("BEAcustomer", "BEAsupplier",    
                  "Econ", "Geo", "Indu", "Pseudo", "Tec", "Analyst")
  
  expected_files <- paste0(output_dir, cross_vars, ".Rds")
  cross_vars <- cross_vars[!file.exists(expected_files) ]
  
  base_path <- "../../../Leadlag/data/Predictionandsignal"
  output_base <- paste0("tmp/covliquidity/", stock_base, "/data/")
  for (crossmom_i in cross_vars) {
    
    message(paste("Processing:", crossmom_i))
    
    matrix_path <- file.path(base_path, crossmom_i, stock_base, "matrix")
    permno_path <- file.path(base_path, crossmom_i, stock_base, "permno")
    
    
    all_files <- dir(path = matrix_path, pattern = "*.Rds")
    if(exists("date_df")) {
      target_files <- paste0(unique(date_df$date), ".Rds")
      datalist <- intersect(all_files, target_files)
    } else {
      datalist <- all_files
    }
    
    if(length(datalist) == 0) {
      message("No files found to process.")
      next
    }
    
    to_dir <- file.path(output_base) 
    dir_create(to_dir, recursive = TRUE)
    
    tic("Batch Execution")
    
    
    nc_used <- ifelse(crossmom_i == "Indu", 2, 9)
    
    plan(multisession, workers =  nc_used)
    future_lapply(datalist, function(f) {
      process_single_file(f, matrix_path, permno_path, msf, crossmom_i, to_dir)
    }, future.seed = TRUE,
    future.scheduling = 5)
    
    toc()
    gc()
    plan(sequential)
    datalist <- dir(path = to_dir,
                    pattern = "*.Rds")
    df_all <- bind_rows(lapply(datalist, function(i){
      gg <-readRDS(file.path(to_dir, i))})) %>%
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
  datalist <- dir(path = to_dir, 
                  pattern = ".Rds")
  library(tidyr)
  data_all =  bind_rows(lapply(datalist, function(i){
    gg <-readRDS(paste0(to_dir, i)) })) %>%
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
                       filter(decom == "p3")%>%
                       dplyr::select(-decom), by = c("liq_measure","name", "part", "date",
                                                     "var", "micro")) %>%
    dplyr::mutate(estimate = estimate.y - estimate.x,
                  decom = "p4") %>%
    dplyr::select(-estimate.x, -estimate.y)
  
  df_all <- bind_rows(df, df_diff) %>%
    data.table()
  
  saveRDS(df_all, file = paste0("tmp/covliquidity/", stock_base, "/pairwise_decom.Rds"))
  
  fit.0 = function(x) {
    m = lm(estimate~1, x)
    v = vcovHC(m, type = "HC0")
    ct = coeftest(m, vcov=v)[1, ]
    out = as.list(ct)
    names(out) = dimnames(ct)[[2]]
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
    dplyr::mutate(per = paste0("$", sprintf("%.2f",round(per*100, 2)), "\\%$")) %>%
    dplyr::mutate(estimate = paste0(sprintf("%.3f",round(estimate, 3)))) %>%
    dplyr::mutate(t = paste0("(", sprintf("%.2f",round(t, 2)), ")")) %>%
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
