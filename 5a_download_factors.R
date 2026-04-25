# download the factors from the French library
library(this.path)
setwd(this.path::this.dir())
source("runmefirst.R")

library(readr)
# library(dplyr)
library(lubridate)
# library(rstudioapi)

# --- Configuration ---
min_date <- ymd("1926-01-01") # FF5 usually starts around here, but 1926 is fine too
max_date <- ymd("2023-12-31")
base_url <- "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"

# --- Helper Function to Download and Clean ---
get_french_data <- function(zip_name, csv_name, skip_rows) {
  # Create a temporary file to store the zip (works on any OS)
  temp_zip <- tempfile(fileext = ".zip")

  # Download
  download.file(paste0(base_url, zip_name), temp_zip, quiet = TRUE, mode = "wb")

  # Read and Clean
  df <- read_csv(unz(temp_zip, csv_name),
    skip = skip_rows,
    col_types = cols(.default = col_double(), date = col_character())
  ) %>%
    rename_all(tolower) %>%
    rename(date = 1) %>%
    mutate_at(vars(-date), as.numeric) %>%
    mutate(date = ymd(parse_date_time(date, "%Y%m"))) %>%
    dplyr::mutate(date = ceiling_date(date %m+% months(0), "month") - days(1)) %>%
    filter(!is.na(date)) %>%
    # Filter Date Range
    filter(date >= min_date, date <= max_date) %>%
    # Convert percentages to decimals (divide numeric cols by 100)
    mutate(across(where(is.numeric), ~ . / 100))

  # Clean up temp file
  unlink(temp_zip)

  return(df)
}

cat("Downloading FF 3 Factors (Base)...\n")
ff_3 <- get_french_data(
  zip_name = "F-F_Research_Data_Factors_CSV.zip",
  csv_name = "F-F_Research_Data_Factors.csv",
  skip_rows = 3
)

cat("Downloading FF 5 Factors...\n")
ff_5_partial <- get_french_data(
  zip_name = "F-F_Research_Data_5_Factors_2x3_CSV.zip",
  csv_name = "F-F_Research_Data_5_Factors_2x3.csv",
  skip_rows = 3
) %>%
  select(date, rmw, cma)

# --- 2. Download Momentum ---
# Contains: Mom
cat("Downloading Momentum...\n")
ff_mom <- get_french_data(
  zip_name = "F-F_Momentum_Factor_CSV.zip",
  csv_name = "F-F_Momentum_Factor.csv",
  skip_rows = 13
)

# --- 3. Download Short-Term Reversal ---
# Contains: ST_Rev
cat("Downloading Reversal...\n")
ff_rev <- get_french_data(
  zip_name = "F-F_ST_Reversal_Factor_CSV.zip",
  csv_name = "F-F_ST_Reversal_Factor.csv",
  skip_rows = 13
) %>%
  rename(strev = 2) # Rename the second column to something safe

# --- 4. Merge All Factors ---
cat("Merging data...\n")
cat("Downloading FF 3 Factors (Base)...\n")
ff_final <- ff_3 %>%
  left_join(ff_mom, by = "date") %>%
  left_join(ff_rev, by = "date") %>%
  left_join(ff_5_partial, by = "date") %>%
  transmute(date, rf, mktRf = `mkt-rf`, smb, hml, rmw, cma, mom, strev)

# --- 5. Save ---
# Determine save location
file_path <- "tmp/factors/"

# Save Final Combined File
output_file <- file.path(file_path, "ff_factors_1926_2023.Rds")
dir.create(file_path, showWarnings = FALSE, recursive = TRUE)
saveRDS(ff_final, file = output_file)

cat("Done! File saved to:", output_file, "\n")
print(head(ff_final))
