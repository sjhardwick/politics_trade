
# load packages
library(arrow)       # for reading & writing parquet files
library(dplyr)       # for general data manipulation
library(furrr)       # for parallel processing
library(data.table)  # for general data manipulation

plan(multisession, workers = 2)  # use two cores

# load data and convert to data.table
gdelt <- read_parquet("data/processed/gdelt_tidy.parquet")
setDT(gdelt)

# require 12+ months of non-zero observations
gdelt <- gdelt[, .SD[sum(gs_adj != 0, na.rm = TRUE) >= 36], by = .(country_1, country_2)]

# function to estimate Q (process variance) from AR(1) model
estimate_Q <- function(data) {
  model <- tryCatch({
    arima(data$gs_adj, order = c(1, 0, 0), include.mean = TRUE)
  }, error = function(e) return(NULL))
  
  if (is.null(model)) return(0.001)  # default fallback Q
  
  max(var(residuals(model), na.rm = TRUE), 1e-6)  # ensure Q is positive
}

# particle filter function
bootstrap_particle_filter <- function(data, N_particles = 1000) {
  
  data <- data[order(month_year)]  # ensure data is ordered
  
  # estimate Q (process variance)
  Q <- estimate_Q(data)
  
  # number of months/time steps in series
  T <- nrow(data)
  
  # initialise particles and weights
  particles <- matrix(0, nrow = T, ncol = N_particles)
  weights <- matrix(1 / N_particles, nrow = T, ncol = N_particles)
  
  # initialise first set of particles using empirical data
  particles[1, ] <- if (!is.na(data$gs_adj[1])) {
    sample(data$gs_adj, N_particles, replace = TRUE)
  } else {
    rnorm(N_particles, mean = 0, sd = sqrt(Q))  # fallback: use mean zero
  }
  
  # pre-compute R_t (observation variance)
  # inversely proportional to the number of events recorded
  data[, R_t := 1 / (num_events + 1)]  # add 1 to avoid division by zero
  
  # loop through each month
  for (t in 2:T) {
    
    # prediction step: propagate particles forward
    particles[t, ] <- sample(particles[t - 1, ], N_particles, replace = TRUE) + 
      rnorm(N_particles, mean = 0, sd = sqrt(Q))
    
    # compute weights based on observation probability
    if (!is.na(data$gs_adj[t])) {
      weights[t, ] <- dnorm(data$gs_adj[t], mean = particles[t, ], sd = sqrt(max(data$R_t[t], 1e-6)))
      
      # ensure the sum of weights is valid
      sum_weights <- sum(weights[t, ], na.rm = TRUE)
      if (is.na(sum_weights) || sum_weights == 0) {
        # print warning if there is issue with weight vector
        warning(sprintf("Zero or NA weights at t = %d for %s-%s", 
                        t, data$country_1[1], data$country_2[1]))
        weights[t, ] <- rep(1 / N_particles, N_particles)  # reset to uniform weights
      } else {
        weights[t, ] <- weights[t, ] / sum_weights  # normalise so weights sum to 1
      }
    } else {
      weights[t, ] <- 1 / N_particles  # assign uniform weights for missing observations
    }
    
    # resample particles
    resample_indices <- sample(1:N_particles, N_particles, replace = TRUE, prob = weights[t, ])
    particles[t, ] <- particles[t, resample_indices]
    weights[t, ] <- 1 / N_particles  # reset particle weights
    
  }
  
  # store results
  data[, filtered_index := rowMeans(particles, na.rm = TRUE)]
  
  return(data)
}

# apply the filter by country pair
filtered_results <- gdelt[, bootstrap_particle_filter(.SD), by = .(country_1, country_2)]

# count nonzero observations
filtered_results[, nonzero_months := sum(gs_adj != 0, na.rm = TRUE), by = .(country_1, country_2)]

# create gsaf_baseline, gsaf_large, gsaf_small based on observation thresholds
filtered_results[, `:=`(
  gsaf_baseline = ifelse(nonzero_months >= 270, filtered_index, NA_real_),
  gsaf_large = ifelse(nonzero_months >= 108, filtered_index, NA_real_),
  gsaf_small = ifelse(nonzero_months >= 432, filtered_index, NA_real_)
)]

# select and rename columns before saving
filtered_results <- filtered_results[, .(
  country_1, 
  country_2, 
  month_year, 
  gsa = gs_adj, 
  gsaf = filtered_index, 
  gsaf_baseline, 
  gsaf_small, 
  gsaf_large
)]

# save results
write_parquet(filtered_results, "data/temp/gdelt_filtered.parquet")
