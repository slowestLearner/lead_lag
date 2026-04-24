# Plot cumulative L/S returns
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")


# set working directory to be the root of the code
setwd(this.path::this.dir())

for (stock_base in c("all", "large")){  
  
  from_dir <- paste0("../tmp/portfolio_results/", stock_base, "/statistics/newey_west/")
  spec_data <- data.table(spec = list.files(from_dir)) %>% mutate(spec = gsub(".RDS", "", spec))
  
  # rank signals
  signal_rank <- readRDS(paste0("../tmp/processed_signals/", stock_base, "/signal_availability.RDS")) %>%
    group_by(var) %>%
    summarize(first_ym = min(yyyymm)) %>%
    ungroup() %>%
    arrange(first_ym) %>%
    mutate(var = gsub("_ret", "", var)) %>%
    bind_rows(., data.table(var = "combined", first_ym = 1)) %>%
    arrange(first_ym) %>%
    mutate(idx = row_number()) %>%
    select(-first_ym) %>%
    mutate(var = ifelse(var == "indu", "industry",
      ifelse(var == "tec", "tech", var)
    )) %>%
    setDT()
  
  # loop through specifications
  for (this_spec in spec_data[, spec]) {
    # this_spec = spec_data[1, spec]
    tic(this_spec)
    
    
    if(this_spec == "remove_20pct_stk_FALSE"){
    data <- readRDS(paste0(from_dir, this_spec, ".RDS")) %>%
      filter(from_hor == 1) %>%
      filter(to_hor <= 120) %>%
      dplyr::rename(hor = to_hor) %>%
      select(-hor_idx, -from_hor)
    }else{
      data <- readRDS(paste0(from_dir, this_spec, ".RDS"))
    }
    
    
    data <- merge(data, signal_rank, by = "var")
  
    to_dir_total <- paste0("../figs/forecasting_return/", stock_base, "/reversals/", this_spec, "/total/")
    to_dir_sep <- paste0("../figs/forecasting_return/", stock_base, "/reversals/", this_spec, "/sep/")
    dir.create(to_dir_total, showWarnings = F, recursive = T)
    dir.create(to_dir_sep, showWarnings = F, recursive = T)
  
    for (this_idx in signal_rank[, idx]) {
      # this_idx = 1
      if(this_spec == "remove_20pct_stk_FALSE"){
      data_this <- data %>% filter(idx == this_idx)
      # add a zero
      data_zero <- data_this %>%
        filter(hor == 1) %>%
        mutate(hor = 0, coef = 0, se = 0)
      data_this <- rbind(data_this, data_zero) %>% mutate(hor = hor / 12)
      }else{
        data_this <- data %>% filter(idx == this_idx)
      }
  
      # plot total
      pp <- ggplot(data_this[var_type == "total"], aes(x = hor, y = coef)) +
        geom_line(lwd = 1) +
        geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
        theme_classic() +
        scale_x_continuous(breaks = 0:10) +
        geom_hline(yintercept = 0, lty = 3) +
        scale_y_continuous(labels = scales::percent_format(1)) +
        labs(x = "Years after portfolio formation", y = "Cumulative return") +
        theme(text = element_text(size = 30))
      ggsave(paste0(to_dir_total, this_idx, "_", signal_rank[idx == this_idx, var], ".png"), pp, "png", width = 4.5, height = 4)
  
      # plot sym vs asy
      data_this <- data_this %>%
        filter(var_type != "total") %>%
        mutate(var_type = ifelse(var_type == "sym", "Symmetric", "Asymmetric"))
  
      pp <- ggplot(data_this, aes(x = hor, y = coef, fill = var_type)) +
        geom_line(lwd = 1, aes(color = var_type)) +
        geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), alpha = .2) +
        theme_classic() +
        scale_x_continuous(breaks = 0:10) +
        geom_hline(yintercept = 0, lty = 3) +
        scale_y_continuous(labels = scales::percent_format(1)) +
        labs(x = "Years after portfolio formation", y = "Cumulative return") +
        theme(legend.title = element_blank(), legend.position = c(.2, .15)) +
        theme(text = element_text(size = 30))
      ggsave(paste0(to_dir_sep, this_idx, "_", signal_rank[idx == this_idx, var], ".png"), pp, "png", width = 4.5, height = 4)
    }
  
    toc()
  }
}