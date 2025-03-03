
plan(multisession, workers = 2)  # use two cores (adjust if needed)

# load packages
library(arrow)      # for reading, writing parquet
library(data.table) # for efficient data handling
library(dlm)        # for kalman filter
library(furrr)      # for parallel map
library(future)     # for parallel backend

# load data and convert to data.table
gdelt <- read_parquet("data/processed/gdelt_tidy.parquet")
gdelt_dt <- as.data.table(gdelt)
remove(gdelt)

# define kalman filter function
apply_kalman <- function(data) {
  tryCatch({
    # sort data by month_year
    data <- data[order(month_year)]
    
    # define local level model builder
    build_dlm <- function(parm) {
      dlmModPoly(order = 1, dV = parm[1], dW = exp(parm[2]))
    }
    
    # initial parameter guess
    init_parm <- c(0, log(0.01))
    
    # build model and set time-varying observation variance
    model <- build_dlm(init_parm)
    model$V <- data$obs_variance # use obs_variance for each time point
    
    # apply kalman filter and smoother
    filtered <- dlmFilter(data$gs_adj, model)
    smoothed <- dlmSmooth(filtered)
    
    # add filtered index
    data$filtered_index <- filtered$f
    # drop the first element (extra initial state) and add smoothed index
    data$smoothed_index <- drop(smoothed$s)[-1]
    return(data)
    
  }, error = function(e) {
    # on error, set smoothed_index to NA and return data
    data$smoothed_index <- NA
    return(data)
  })
}

# clean function to ensure output is a data.table without duplicate columns
apply_kalman_clean <- function(data) {
  # ensure data is a data.table
  data <- as.data.table(data)
  result <- apply_kalman(data)
  # remove duplicate columns if any
  result <- result[, unique(names(result)), with = FALSE]
  return(result)
}

# define function to filter dataset based on observation thresholds
# and apply kalman filter
apply_filters_and_kalman <- function(non_zero_threshold, zero_streak_threshold) {
  gdelt_dt_filtered <- gdelt_dt[, {
    
    # count non-zero observations
    non_zero_count <- sum(gs_adj != 0, na.rm = TRUE)
    
    # identify longest consecutive zero streak
    zero_run_lengths <- rle(gs_adj == 0)
    max_zero_streak <- if (any(zero_run_lengths$values)) {
      max(zero_run_lengths$lengths[zero_run_lengths$values], na.rm = TRUE)
    } else {
      0
    }
    
    # flag valid series
    valid_series <- (non_zero_count >= non_zero_threshold) & 
      (max_zero_streak <= zero_streak_threshold)
    
    if (valid_series) {
      obs_variance <- ifelse(num_events == 0, 1e6, 1 / num_events)
      .(month_year, country_1, country_2, gs_adj, num_events, obs_variance)
    } else {
      NULL
    }
    
  }, by = .(country_1, country_2)]
  
  # split data by country pair
  gdelt_split <- split(gdelt_dt_filtered, 
                       list(gdelt_dt_filtered$country_1, gdelt_dt_filtered$country_2), 
                       drop = TRUE)
  
  # apply kalman filter in parallel
  gdelt_list <- future_map(gdelt_split, apply_kalman_clean, .progress = TRUE)
  
  # bind results
  gdelt_filtered <- rbindlist(gdelt_list, use.names = TRUE, fill = TRUE)
  
  # convert month_year to date format
  gdelt_filtered[, month_year := as.IDate(paste0(substr(month_year, 1, 4), "-", 
                                                 substr(month_year, 5, 6), "-01"))]
  
  return(gdelt_filtered)
}

# define thresholds for three versions (one baseline, two robustness checks)
thresholds <- list(
  # larger sample: 20 per cent minimum non-zero obs, 18 months max zero streak
  list(non_zero_threshold = 108, zero_streak_threshold = 18), 
  # standard/baseline sample: 50 per cent, 12 months
  list(non_zero_threshold = 270, zero_streak_threshold = 12),
  # stricter/smaller sample: 90 per cent, 6 months
  list(non_zero_threshold = 486, zero_streak_threshold = 6)
)

# run each version in parallel
gdelt_versions <- future_map(thresholds, 
                             ~ apply_filters_and_kalman(.x$non_zero_threshold, .x$zero_streak_threshold),
                             .progress = TRUE)

# convert list to named objects for each version of data
list2env(setNames(gdelt_versions, c("gdelt_larger", "gdelt_baseline", "gdelt_stricter")), envir = .GlobalEnv)

# convert month_year to date format in original data
gdelt_dt[, month_year := as.IDate(paste0(substr(month_year, 1, 4), "-", 
                                         substr(month_year, 5, 6), "-01"))]

# compile into one table
# start with the main dataset
gdelt <- gdelt_dt[, .(country_1, country_2, month_year, gs_adj)]
setnames(gdelt, "gs_adj", "gsa")

# merge baseline, larger and smaller samples
gdelt <- merge(
  gdelt,
  gdelt_baseline[, .(country_1, country_2, month_year, gsaf = filtered_index)],
  by = c("country_1", "country_2", "month_year"),
  all.x = TRUE
)

gdelt <- merge(
  gdelt,
  gdelt_larger[, .(country_1, country_2, month_year, gsaf_large = filtered_index)],
  by = c("country_1", "country_2", "month_year"),
  all.x = TRUE
)

gdelt <- merge(
  gdelt,
  gdelt_stricter[, .(country_1, country_2, month_year, gsaf_small = filtered_index)],
  by = c("country_1", "country_2", "month_year"),
  all.x = TRUE
)

# save merged data
write_parquet(gdelt, "data/processed/gdelt_filtered.parquet")
