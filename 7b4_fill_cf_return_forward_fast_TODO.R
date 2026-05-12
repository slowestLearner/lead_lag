# ---- Fast version of 7b4_fill_cf_return_forward.R
# Same outputs: wide-matrix propagation replaces repeated data.table self-merges.
library(this.path)
setwd(this.path::this.dir())
os_type <- Sys.info()["sysname"]

if (os_type == "Windows") {
  source("../runmefirst.R")
} else {
  source("~/.runmefirst")
}

raw_data <- readRDS("tmp/valuation/implied_forward_cf_return.RDS")
statpers_to_idx <- unique(raw_data[, .(statpers)])[order(statpers)][, idx := .I]

fill_horizons <- c(1, 2, 4, 8, 20, 40)

to_dir <- "tmp/valuation/horizons_fast/"
dir.create(to_dir, showWarnings = FALSE, recursive = TRUE)

# --- prep once: balanced idx x permno x hor 1..40 (columns "1".."40") ---
tic()
prep <- merge(raw_data, statpers_to_idx, by = "statpers")[, statpers := NULL]
prep[, hor := factor(hor, levels = 1:40)]
wide <- dcast(prep, idx + permno ~ hor, value.var = "logrethat", drop = FALSE)

cn <- as.character(1:40)
missing_cn <- setdiff(cn, names(wide))
for (nm in missing_cn) {
  wide[, (nm) := NA_real_]
}
setcolorder(wide, c("idx", "permno", cn))

n <- nrow(wide)
idx <- wide[["idx"]]
permno <- wide[["permno"]]
base_lt <- as.matrix(wide[, ..cn])
toc()

#' Forward-fill along horizon columns; same rule as original loop:
#' fill NA from immediate lower hor if donor age < h_limit.
#' Returns list(lt, ag): ag is carry depth for filled cells (0 for untouched originals).
fill_cf_forward_mat <- function(lt, h_limit, n_iter = 39L) {
  ag <- matrix(0L, nrow = nrow(lt), ncol = ncol(lt))
  for (.iter in seq_len(n_iter)) {
    prev_lt <- cbind(NA_real_, lt[, 1L:39L, drop = FALSE])
    prev_ag <- cbind(NA_real_, ag[, 1L:39L, drop = FALSE])
    mask <- is.na(lt) & !is.na(prev_lt) & !is.na(prev_ag) & (prev_ag < h_limit)
    lt[mask] <- prev_lt[mask]
    ag[mask] <- prev_ag[mask] + 1L
  }
  list(lt = lt, ag = ag)
}

#' One unconstrained run (h_limit large enough for hor 1..40), then for each cap K
#' keep imputed values iff carry depth ag <= K. Same result as re-running with
#' prev_age < K because propagation is a single left-to-right path per firm-month.
uncap <- max(40L, max(fill_horizons))
full <- fill_cf_forward_mat(matrix(base_lt, nrow = n, ncol = 40L), uncap)
lt_full <- full$lt
ag_full <- full$ag

wide_to_long <- function(lt, idx_vec, permno_vec) {
  data.table(
    idx = rep(idx_vec, each = 40L),
    permno = rep(permno_vec, each = 40L),
    hor = rep.int(1L:40L, length(idx_vec)),
    logrethat = as.vector(t(lt))
  )
}

for (h_limit in fill_horizons) {
  # h_limit <- fill_horizons[2]
  message("Processing fill-forward horizon: ", h_limit)

  lt_out <- matrix(base_lt, nrow = n, ncol = 40L)
  keep <- is.na(base_lt) & !is.na(lt_full) & (ag_full <= h_limit)
  lt_out[keep] <- lt_full[keep]

  data <- wide_to_long(lt_out, idx, permno)
  data <- merge(data, statpers_to_idx, by = "idx")[, idx := NULL]
  data <- na.omit(data)

  saveRDS(data, paste0(to_dir, "implied_forward_cf_return_filled_", h_limit, ".RDS"))
}
