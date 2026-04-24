# Use Fama-MacBeth to estimate return predictive power. Progressively control for stuff
source("runmefirst.R")
library(this.path)
setwd(this.path::this.dir()) # set working directory to be the root of the code

# -- get signals
for (stock_base in c("all", "large")){
  tic("loading data")
  if (.Platform$OS.type == "windows") {
    base_dir <- paste0("D:/Dropbox/Leadlag/data/signal_demean/", stock_base)
  } else {
    base_dir <- paste0("~/Dropbox/Leadlag/data/signal_demean/", stock_base)
  }
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
  
  data_list <- lapply(names(file_map), function(f_name) {
    
    f_path <- file.path(base_dir, f_name)
    
    if (!file.exists(f_path)) {
      warning(paste("File not found:", f_path))
      return(NULL)
    }
    dt <- readRDS(f_path)
    setDT(dt)
    dt[, var := file_map[[f_name]]]
    
    dt[, signal := signal_s + signal_a]
    dt[, c("signal_s", "signal_a") := NULL] 
    
    
    return(dt)
  })
  
  data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  
  # Cleanup
  rm(data_list)
  gc()
  
  # also need the combined signal
  data_combined <- data[, .(var = "combined", signal = mean(signal)), .(yyyymm, permno)]
  data <- rbind(data, data_combined)
  rm(data_combined)
  
  # NEW: make signals z-scores?
  data[, signal := Winsorize(signal, probs = c(.005, .995)), .(yyyymm, var)]
  data[, signal := (signal - mean(signal)) / sd(signal), .(yyyymm, var)]
  
  # get next-period return. keep track of which stocks are large (above 20% size cutoff)
  ret_data <- readRDS("../../data/Stocks/Monthly_CRSP.RDS") %>%
    mutate(factor_model = "raw") %>%
    select(yyyymm, permno, keep, ret)
  
  # change timing to start from 1m after the signals
  tmp <- ret_data[, .(yyyymm)] %>%
    unique() %>%
    mutate(mm = yyyymm - 100 * floor(yyyymm / 100)) %>%
    mutate(yyyymm_prev = if_else(mm == 1, yyyymm - 100 + 11, yyyymm - 1)) %>%
    select(-mm)
  
  ret_data <- merge(ret_data, tmp, by = "yyyymm") %>%
    mutate(yyyymm = yyyymm_prev) %>%
    select(-yyyymm_prev) %>%
    dplyr::rename(ret1 = ret) %>%
    na.omit() %>%
    setDT()
  rm(tmp)
  setDT(data)
  setDT(ret_data)
  setkey(data, yyyymm, permno)
  setkey(ret_data, yyyymm, permno)
  base_data <- ret_data[data, 
                        on = .(yyyymm, permno), 
                        allow.cartesian = TRUE, 
                        nomatch = 0]
  # base_data <- merge(data, ret_data, by = c("yyyymm", "permno"), allow.cartesian = T)
  rm(data, ret_data)
  
  # get stock characteristics
  char_data <- readRDS("../../data/Stocks/Characteristics/andrew_chen_characteristics.RDS")
  vars_char <- setdiff(names(char_data), c("yyyymm", "permno"))
  
  # get industry dummies
  ind_data <- readRDS("../../data/Stocks/Characteristics/ff12_industries.RDS")
  vars_ind <- setdiff(names(ind_data), c("yyyymm", "permno"))
  
  # to make apples to apples compairson, should make these also z-scores
  for (this_var in vars_char) {
    print(this_var)
    setnames(char_data, this_var, "xx")
    char_data[!is.na(xx), xx := (xx - mean(xx)) / sd(xx), yyyymm]
    setnames(char_data, "xx", this_var)
  }
  
  for (this_var in vars_ind) {
    print(this_var)
    setnames(ind_data, this_var, "xx")
    ind_data[!is.na(xx), xx := (xx - mean(xx)) / sd(xx), yyyymm]
    setnames(ind_data, "xx", this_var)
  }
  
  char_data[is.na(char_data)] <- 0
  ind_data[is.na(ind_data)] <- 0
  toc()
  
  # --- model specifications
  spec_data <- data.table(model_name = c("none", "FF3", "FF3_ind", "FF3_ind_char")) %>%
    mutate(spec_idx = row_number()) %>%
    setDT()
  spec_data[spec_idx == 1, model_formula := "ret1 ~ signal"]
  spec_data[spec_idx == 2, model_formula := "ret1 ~ signal + beta + size + bm"]
  spec_data[spec_idx == 3, model_formula := paste0("ret1 ~ signal + beta + size + bm + ", paste0(vars_ind, collapse = " + "))]
  spec_data[spec_idx == 4, model_formula := paste0("ret1 ~ signal + ", paste0(c(vars_char, vars_ind), collapse = " + "))]
  
  # --- loop over variables
  vars <- unique(base_data[, var])
  
  # with parallelization, each var is around 10 secs
  out_all <- data.table() # record results here
  for (this_var in vars) {
    tic(this_var)
    # this_var <- vars[1]
  
    # merge all together, fill zeros
    data <- copy(base_data[var == this_var]) %>%
      left_join(char_data, by = c("yyyymm", "permno")) %>%
      left_join(ind_data, by = c("yyyymm", "permno")) %>%
      setDT()
    data[is.na(data)] <- 0
  
    # fit models by period
    data_list <- split(data, by = c("yyyymm", "var"))
    rm(data)
    gc()
  
    # estimate FM for one and report coefficients
    p.one_fm <- function(this_data) {
      # this_data <- data_list[[1]]
  
      out <- data.table()
  
      # full universe
      for (this_spec in spec_data[, spec_idx]) {
        this_model_formula <- spec_data[spec_idx == this_spec, model_formula]
        mm <- lm(as.formula(this_model_formula), this_data)
        out <- rbind(out, data.table(
          universe = "full",
          spec_idx = this_spec,
          reg_var = names(mm$coef),
          coef = mm$coef,
          n = nrow(this_data),
          r2 = var(mm$fitted.values) / this_data[, var(ret1)]
        ))
      }
  
      # just large stocks
      for (this_spec in spec_data[, spec_idx]) {
        this_model_formula <- spec_data[spec_idx == this_spec, model_formula]
        mm <- lm(as.formula(this_model_formula), this_data[keep == 1])
        out <- rbind(out, data.table(
          universe = "ge_nyse_20pct_cutoff",
          spec_idx = this_spec,
          reg_var = names(mm$coef),
          coef = mm$coef,
          n = nrow(this_data[keep == 1]),
          r2 = var(mm$fitted.values) / this_data[keep == 1, var(ret1)]
        ))
      }
  
      out[, yyyymm := this_data[1, yyyymm]]
      out[, var := this_data[1, var]]
  
      return(out)
    }
  
    # process in blocks
    processing <- data.table(idx = 1:length(data_list))
    block_size <- 100
    processing[, block_idx := ceiling(idx / block_size)]
  
    #out <- data.table()
    all_blocks <- vector("list", max(processing$block_idx))
    for (this_block in 1:max(processing[, block_idx])) {
      # tic(paste0("Processing block ", this_block, " of ", max(processing[, block_idx]), " for var = ", this_var))
      #out <- rbind(out, rbindlist(mclapply(data_list[processing[block_idx == this_block, idx]], p.one_fm, mc.cores = nc)))
      # toc()
      chunk_indices <- processing[block_idx == this_block, idx]
      chunk_data <- data_list[chunk_indices]
      tic(paste0("Processing block ", this_block, " of ", max(processing[, block_idx]), " for var = ", this_var))
      block_results <- future_lapply(
        chunk_data, 
        p.one_fm, 
        future.packages = c("data.table", "sandwich", "lmtest") 
      )
      all_blocks[[this_block]] <- rbindlist(block_results)
      rm(chunk_data, block_results)
      toc()
    }
    out <- rbindlist(all_blocks)
    out_all <- rbind(out_all, out)
    rm(out);gc()
    toc()
  }
  
  out_all <- merge(out_all, spec_data[, .(model_name, spec_idx)], by = "spec_idx")
  
  # save the by-month result
  to_dir <- paste0("tmp/other_methods/", stock_base, "/fama_macbeth/")
  dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)
  saveRDS(out_all, paste0(to_dir, "fm_results_by_month.RDS"))
  
  # --- summarize
  # out_all <- readRDS(paste0(to_dir, "fm_results_by_month.RDS"))
  
  out <- out_all[, .(
    n = round(mean(n)),
    r2 = mean(r2),
    coef = mean(coef, na.rm = T),
    se = sd(coef, na.rm = T) / sqrt(sum(!is.na(coef)))
  ), .(spec_idx, model_name, universe, var, reg_var)]
  
  saveRDS(out, paste0(to_dir, "fm_results_summary.RDS"))
}