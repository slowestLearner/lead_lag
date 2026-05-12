# ---- Updated 2a4_fill_cf_return_forward.R
library(this.path)
setwd(this.path::this.dir())
os_type <- Sys.info()["sysname"]

# Set the file path based on the operating system
if (os_type == "Windows") {
  # For Windows
  source("../runmefirst.R")
} else {
  # For macOS and Linux
  source("~/.runmefirst")
}

raw_data <- readRDS("tmp/valuation/implied_forward_cf_return.RDS")
statpers_to_idx <- unique(raw_data[, .(statpers)])[order(statpers)][, idx := .I]

# Define the horizons to test
fill_horizons <- c(1, 2, 4, 8, 20, 40)

to_dir <- "tmp/valuation/horizons/"
dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

for (h_limit in fill_horizons) {
  # h_limit <- 1
  message(paste("Processing fill-forward horizon:", h_limit))

  data <- merge(raw_data, statpers_to_idx, by = "statpers")[, statpers := NULL]
  data <- dcast(data, idx + permno ~ hor, value.var = "logrethat")
  setnames(data, paste0(1:40), paste0("logrethat", 1:40))
  data <- melt(data, id.vars = c("idx", "permno"), variable.name = "hor", value.name = "logrethat")
  data[, hor := as.integer(gsub("logrethat", "", as.character(hor)))]

  data[, age := 0]

  for (i in 1:39) {
    tic(i)
    data <- merge(data, data[, .(idx, permno,
      hor = hor + 1,
      prev_val = logrethat,
      prev_age = age
    )],
    by = c("idx", "permno", "hor"), all.x = T
    )

    # Use h_limit as the parameter (prev_age < h_limit)
    data[
      is.na(logrethat) & !is.na(prev_val) & prev_age < h_limit,
      `:=`(logrethat = prev_val, age = prev_age + 1)
    ]

    data[, c("prev_val", "prev_age") := NULL]
    toc()
  }

  data <- merge(data, statpers_to_idx, by = "idx")[, idx := NULL] %>% na.omit()



  saveRDS(data, paste0(to_dir, "implied_forward_cf_return_filled_", h_limit, ".RDS"))
}
