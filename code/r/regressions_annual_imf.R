
# load packages
library(arrow)
library(fixest)
library(purrr)
library(tidyverse)

# load data
data <- read_parquet("data/processed/annual_data_imf.parquet")

# source the regression function
source("code/r/regression_functions.R")

# RHS variable options
rhs_vars_list <- list(
  
  # GATT/WTO/RTAs
  c("log_ipd", "rta", "gatt_wto_one", "gatt_wto_both", 
    "I(log_ipd * rta)",
    "I(log_ipd * gatt_wto_one)", 
    "I(log_ipd * gatt_wto_both)"),
  
  # domestic institutional indicators
  c("log_ipd",
    "I(log(v2x_corr_o) * log_ipd)",
    "I(log(v2x_corr_d) * log_ipd)", 
    "I(log(polity_scaled_o) * log_ipd)",
    "I(log(polity_scaled_d) * log_ipd)"),
  
  # all of the above
  c("log_ipd", "rta", "gatt_wto_one", "gatt_wto_both", 
    "I(log_ipd * rta)",
    "I(log_ipd * gatt_wto_one)", 
    "I(log_ipd * gatt_wto_both)",
    "I(log(v2x_corr_o) * log_ipd)",
    "I(log(v2x_corr_d) * log_ipd)", 
    "I(log(polity_scaled_o) * log_ipd)",
    "I(log(polity_scaled_d) * log_ipd)")
  
)

# define regressions
regression_specs <- tibble(
  dep_var = "trade_flow",
  rhs_vars = rep(rhs_vars_list, 6),
  start_year = c(rep(1948, 3), rep(1980, 3), rep(1948, 3), rep(1980, 3), rep(1995, 3), rep(2009, 3)),  
  end_year = c(rep(2023, 3), rep(2023, 3), rep(1979, 3), rep(1994, 3), rep(2008, 3), rep(2023, 3))
)

# run regressions
results <- pmap(regression_specs, function(dep_var, rhs_vars, start_year, end_year) {
  run_trade_regression(
    data = data,
    dep_var = dep_var,
    rhs_vars = rhs_vars,
    start_year = start_year,
    end_year = end_year
  )
})

# save results to text file
save_results_to_txt(results, regression_specs, filename = "a_ipd_imf.txt")
