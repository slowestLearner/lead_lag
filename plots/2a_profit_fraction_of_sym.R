# plot the fraction of profits due to asymmetric
library(sandwich)
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")
for (stock_base in c("all", "large")){ 
# already computed
  data <- readRDS(paste0("../tmp/portfolio_results/", stock_base,
                         "/statistics/newey_west/remove_20pct_stk_FALSE.RDS")) %>%
    filter((from_hor == 1) & (to_hor %in% c(1, 12))) %>%
    select(hor = to_hor, var, var_type, coef, se) %>%
    filter(!grepl("combined_", var)) %>%
    filter(var_type != "sym") %>%
    mutate(var_type = ifelse(var_type == "asy", "Asymmetric CSM", "Total CSM"))
  
  # rank the predictors
  signal_rank <- readRDS(paste0("../tmp/processed_signals/", stock_base, 
                                "/signal_availability.RDS")) %>%
    group_by(var) %>%
    summarize(first_ym = min(yyyymm), var_lab = first(var_lab)) %>%
    ungroup() %>%
    arrange(first_ym) %>%
    mutate(var = gsub("_ret", "", var)) %>%
    bind_rows(., data.table(var = "combined", var_lab = "Combined CSM", first_ym = 1)) %>%
    arrange(first_ym) %>%
    mutate(idx = row_number()) %>%
    select(-first_ym) %>%
    mutate(var = ifelse(var == "indu", "industry",
      ifelse(var == "tec", "tech", var)
    )) %>%
    setDT()
  
  data_all <- merge(data, signal_rank, by = "var")
  rm(data, signal_rank)
  
  for (this_hor in unique(data_all[, hor])) {
    data <- copy(data_all[hor == this_hor]) %>% arrange(var_type, idx)
  
    # # how to make the error bars centered?
    # pp <- ggplot(data, aes(x = idx, y = coef, fill = var_type)) +
    #   geom_bar(stat = "identity", position = "dodge") +
    #   geom_errorbar(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = "dodge", alpha = .5, width = .5) +
    #   theme_classic() +
    #   scale_y_continuous(labels = scales::percent_format(0.1)) +
    #   scale_x_discrete(limits = factor(data[var_type == first(var_type), idx]), labels = data[var_type == first(var_type), var_lab]) +
    #   labs(x = element_blank(), y = "Portfolio return") +
    #   theme(legend.position = c(.2, .8), legend.title = element_blank()) +
    #   theme(text = element_text(size = 25), axis.text.x = element_text(angle = 45, hjust = 1))
  
    # 1. Define the dodge width explicitly
    pd <- position_dodge(width = 0.9)
  
    pp <- ggplot(data, aes(x = idx, y = coef, fill = var_type)) +
      # 2. Apply 'pd' to the bars
      geom_bar(stat = "identity", position = pd) +
  
      # 3. Apply 'pd' to the error bars
      # Note: 'width = 0.5' here controls the whisker size, not the position
      geom_errorbar(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se),
        position = pd, alpha = .5, width = 0.5
      ) +
      theme_classic() +
      scale_y_continuous(labels = scales::percent_format(0.1)) +
      scale_x_discrete(
        limits = factor(data[var_type == first(var_type), idx]),
        labels = data[var_type == first(var_type), var_lab]
      ) +
      labs(x = element_blank(), y = "Portfolio return") +
      theme(legend.position = c(.2, .8), legend.title = element_blank()) +
      theme(text = element_text(size = 25), axis.text.x = element_text(angle = 45, hjust = 1))
  
    to_dir <- paste0("../figs/understanding/", stock_base, "/asy/fraction/")
    dir.create(to_dir, showWarnings = F, recursive = T)
    to_file <- paste0(to_dir, "returns_hor_", this_hor, ".png")
    ggsave(to_file, pp, "png", width = 4, height = 4)
  }
}