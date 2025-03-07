
# load packages
library(arrow)
library(data.table)
library(fixest)
library(purrr)
library(tidyverse)

# source the regression function
source("code/r/regression_functions.R")

# load data
data <- read_parquet("data/processed/annual_data_tp.parquet")

# aggregate data but only if there are non-NA observations for each industry

# filter to keep only (year, pair) groups with all nine industries
valid_counts <- data[, .(count = sum(!is.na(trade_comb))), by = .(year, pair)]
valid_pairs <- valid_counts[count == 9, .(year, pair)]

# merge with original data to keep only valid (year, pair) groups
filtered_data <- data[valid_pairs, on = .(year, pair)]

# select columns to keep, excluding any industry-specific ones
cols_to_keep <- setdiff(names(data), c("trade_comb", "industry", "year_o_industry", "year_d_industry", "pair_industry"))

# aggregate: sum trade_comb, count non-zero industries, retain first value of other variables
data <- filtered_data[, 
                      c(
                        list(
                          trade_comb = sum(trade_comb, na.rm = TRUE),  # total trade for (year, pair)
                          trade_count = sum(trade_comb > 0, na.rm = TRUE)  # count industries with trade > 0
                        ), 
                        lapply(.SD, first)  # keep first occurrence of other variables
                      ), 
                      by = .(year, pair), 
                      .SDcols = cols_to_keep
]

# compute average trade per industry; set to zero if no industries with trade
data[, trade_avg := fifelse(trade_count > 0, trade_comb / trade_count, 0)]

# add new columns: year_o, year_d, and pair
data[, year_o := paste(year, iso_o, sep = "-")]
data[, year_d := paste(year, iso_d, sep = "-")]

# remove duplicate year and pair columns
data <- data[, -c(1, 2), with = FALSE]

# clean up memory
remove(filtered_data, valid_counts, valid_pairs)
gc()

# save/load backup as needed
# write_parquet(data, "data/processed/annual_data_tp_aggregated.parquet")
data <- read_parquet("data/processed/annual_data_tp_aggregated.parquet")

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
  dep_var = c(rep("trade_count", 6), rep("trade_avg", 6)),
  rhs_vars = rep(list(c("log_ipd", "rta", "gatt_wto_one", "gatt_wto_both", 
                        "I(log_ipd * rta)",
                        "I(log_ipd * gatt_wto_one)", 
                        "I(log_ipd * gatt_wto_both)",
                        "I(log(v2x_corr_o) * log_ipd)",
                        "I(log(v2x_corr_d) * log_ipd)", 
                        "I(log(polity_scaled_o) * log_ipd)",
                        "I(log(polity_scaled_d) * log_ipd)")), 12),
  start_year = c(1960, 1980, 1960, 1980, 1995, 2009,
                 1960, 1980, 1960, 1980, 1995, 2009),  
  end_year   = c(2020, 2020, 1979, 1994, 2008, 2020,
                 2020, 2020, 1979, 1994, 2008, 2020)
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
save_results_to_txt(results, regression_specs, filename = "a_ipd_tp_margins.txt")
