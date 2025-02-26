
library(arrow)      # for reading, writing parquet
library(data.table) # for efficient data handling
library(dlm)        # for kalman filter
library(furrr)      # for parallel map
library(future)     # for parallel backend
library(tidyverse)  # for general data manipulation 
library(zoo)        # for date formatting

# load data and convert to data.table
gdelt <- read_parquet("data/processed/gdelt_tidy.parquet")
gdelt_dt <- as.data.table(gdelt)
remove(gdelt)

# define thresholds
non_zero_threshold <- 270  # i.e. must have at least half of 540 months
zero_streak_threshold <- 12  # maximum consecutive zeroes allowed

# apply filtering and add observational variance for kalman filter
gdelt_dt <- gdelt_dt[, {
  
  # count non-zero observations
  non_zero_count <- sum(gs_adj != 0, na.rm = TRUE)
  
  # identify longest consecutive zero stretch
  zero_run_lengths <- rle(gs_adj == 0)
  
  # if there are any zeros, get the maximum length; otherwise 0
  max_zero_streak <- if (any(zero_run_lengths$values)) {
    max(zero_run_lengths$lengths[zero_run_lengths$values], na.rm = TRUE)
  } else {
    0  # no zeroes in the series
  }
  
  # flag series that meet both conditions
  valid_series <- (non_zero_count > non_zero_threshold) & 
    (max_zero_streak < zero_streak_threshold)
  
  if (valid_series) {
    # compute obs_variance: large value for zero events, inverse for others
    obs_variance <- ifelse(num_events == 0, 1e6, 1 / num_events)
    # return necessary columns
    .(month_year, country_1, country_2, gs_adj, num_events, obs_variance)
    
  } else {
    NULL  # drop the country pair if it fails either condition
  }
  
}, by = .(country_1, country_2)]

# set up parallel processing
available_cores <- parallel::detectCores() - 1  # leave one core free
plan(multisession, workers = available_cores)

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

# split data by country pair
gdelt_split <- split(gdelt_dt, 
                     list(gdelt_dt$country_1, gdelt_dt$country_2), 
                     drop = TRUE)

# clean function to ensure output is a data.table without duplicate columns
apply_kalman_clean <- function(data) {
  # ensure data is a data.table
  data <- as.data.table(data)
  result <- apply_kalman(data)
  # remove duplicate columns if any
  result <- result[, unique(names(result)), with = FALSE]
  return(result)
}

# process in parallel and return a list of data.tables
gdelt_list <- future_map(
  gdelt_split,
  apply_kalman_clean,
  .progress = TRUE
)

# bind the results
gdelt_filtered <- rbindlist(gdelt_list, use.names = TRUE, fill = TRUE)

# convert month_year to date format
gdelt_filtered[, month_year := as.IDate(paste0(substr(month_year, 1, 4), "-", 
                                               substr(month_year, 5, 6), "-01"))]

# save smoothed data to parquet
write_parquet(gdelt_filtered, "data/processed/gdelt_filtered.parquet")

