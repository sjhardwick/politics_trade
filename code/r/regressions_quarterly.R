
# load packages
library(arrow)
library(fixest)
library(purrr)
library(tidyverse)
library(zoo)

# load data
data <- read_parquet("data/processed/quarterly_data.parquet")

# add year column
data$year <- as.integer(substr(data$year_qtr, 1, 4))

# source the regression function
source("code/r/regression_functions.R")

# RHS variable options
rhs_vars_list <- list(
  # GATT/WTO/RTAs
  c("ihs_gsaf", "rta", "gatt_wto_one", "gatt_wto_both", 
    "I(ihs_gsaf * rta)",
    "I(ihs_gsaf * gatt_wto_one)", 
    "I(ihs_gsaf * gatt_wto_both)"),
  
  # domestic institutional indicators
  c("ihs_gsaf",
    "I(log(v2x_corr_o) * ihs_gsaf)",
    "I(log(v2x_corr_d) * ihs_gsaf)", 
    "I(log(polity_scaled_o) * ihs_gsaf)",
    "I(log(polity_scaled_d) * ihs_gsaf)"),
  
  # all of the above
  c("ihs_gsaf", "rta", "gatt_wto_one", "gatt_wto_both", 
    "I(ihs_gsaf * rta)",
    "I(ihs_gsaf * gatt_wto_one)", 
    "I(ihs_gsaf * gatt_wto_both)",
    "I(log(v2x_corr_o) * ihs_gsaf)",
    "I(log(v2x_corr_d) * ihs_gsaf)", 
    "I(log(polity_scaled_o) * ihs_gsaf)",
    "I(log(polity_scaled_d) * ihs_gsaf)")
)

# define regressions
regression_specs <- tibble(
  dep_var = "trade_flow",
  rhs_vars = rep(rhs_vars_list, 4),
  start_year = c(rep(1980, 3), rep(1980, 3), rep(1995, 3), rep(2009, 3)),  
  end_year   = c(rep(2023, 3), rep(1994, 3), rep(2008, 3), rep(2023, 3))
)

# run regressions
results <- pmap(regression_specs, function(dep_var, rhs_vars, start_year, end_year, fe) {
  run_trade_regression(
    data = data,
    dep_var = dep_var,
    rhs_vars = rhs_vars,
    start_year = start_year,
    end_year = end_year,
    fe = c("year_qtr_o", "year_qtr_d", "pair", "border_year_qtr")
  )
})

# save results to text file
save_results_to_txt(results, regression_specs, filename = "q_gdelt_imf.txt")
