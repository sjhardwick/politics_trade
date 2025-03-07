
# function to automate running regressions
run_trade_regression <- function(data, dep_var, rhs_vars,
                                 start_year = NULL, end_year = NULL,
                                 fe = c("year_o", "year_d", "pair", "border_year"),
                                 cluster = "pair") {
  
  # filter data to chosen year range, if needed
  if (!is.null(start_year) & !is.null(end_year)) {
    data <- data %>% filter(year >= start_year & year <= end_year)
  }
  
  # ensure rhs_vars is not empty
  if (length(rhs_vars) > 0) {
    rhs <- paste(rhs_vars, collapse = " + ")
  } else {
    stop("Error: RHS variables must be provided.")
  }
  
  # create regression formula in fixest style
  regression_formula <- as.formula(paste(dep_var, "~", rhs))
  
  # pass fixed effects (fixef) as a character vector (otherwise won't work)
  fixef_vector <- fe
  
  # run fixest regression
  model <- tryCatch({
    fixest::fepois(
      fml = regression_formula,
      data = data,
      fixef = fixef_vector,
      cluster = cluster
    )
  }, error = function(e) {
    message("Error in regression: ", e$message)
    return(NULL)  # return null if error occurs
  })
  
  return(model)
}


# function to save regression results with start and end years
save_results_to_txt <- function(results, specs, save_path = "results", filename = "all_regression_results.txt") {
  
  # ensure results folder exists
  if (!dir.exists(save_path)) {
    dir.create(save_path)
  }
  
  # define the full file path
  filepath <- file.path(save_path, filename)
  
  # open file connection to overwrite existing file
  sink(filepath)
  
  # loop through each result and print summary
  for (i in seq_along(results)) {
    cat("\n=============================================\n")
    cat(paste("MODEL", i, "SUMMARY:\n"))
    cat("=============================================\n")
    
    # print the time period of the model
    cat("Time period: ", specs$start_year[i], "-", specs$end_year[i], "\n\n")
    
    # print the model summary
    print(summary(results[[i]]))
    
    cat("\n\n")  # add space between models
  }
  
  # close file output
  sink()
  
  message(paste("All results saved to:", filepath))
}
