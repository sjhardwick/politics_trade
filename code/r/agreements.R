
library(arrow)         # write parquet
library(countrycode)   # convert country name to iso
library(stringr)       # convert text to date
library(lubridate)     # convert text to date
library(tidyverse)     # general data manipulation
library(zoo)           # for year-quarter class

# load in raw spreadsheet from WTO website (27 feb 2025)
ftas_raw <- read_csv("data/raw/wto_agreements.csv")

fta_dates <- ftas_raw %>% select(7, 9)
colnames(fta_dates) <- c("date_eif", "signatories")

# function to extract earlier date when there are multiple dates for an FTA
# (e.g. goods entry into force vs. services)
extract_earlier_date <- function(date_string) {
  # match dates with either 2-digit or 4-digit year using regex
  pattern <- "\\d{1,2}-[A-Za-z]{3}-\\d{2,4}"
  date_matches <- regmatches(date_string, gregexpr(pattern, date_string))[[1]]
  
  if (length(date_matches) == 0) {
    return(NA)
  }
  
  # convert each matched date string into a date object
  date_objs <- sapply(date_matches, function(x) {
    # determine if the year part is 2 or 4 digits by splitting the string
    parts <- strsplit(x, "-")[[1]]
    year_part <- parts[3]
    if (nchar(year_part) == 2) {
      as.Date(x, format = "%d-%b-%y")
    } else {
      as.Date(x, format = "%d-%b-%Y")
    }
  })
  
  # return earliest date
  min(date_objs)
}

# tidy up dates column
fta_dates <- fta_dates %>%
  mutate(date_eif = as.Date(sapply(date_eif, extract_earlier_date))) %>%
  mutate(fta_id = c(1:nrow(fta_dates)))

fta_dates_long <- fta_dates %>%
  # split signatories into one row per country using semicolon & optional space
  separate_rows(signatories, sep = ";\\s*") %>%
  # change some country names for easier conversion to iso 3
  mutate(signatory_name = case_when(signatories == "Trkiye" ~ "Turkiye", 
                                    signatories == "Lebanese Republic" ~ "Lebanon",
                                    TRUE ~ signatories)) %>%
  # convert signatory name to iso 3 code
  mutate(signatory_iso = countrycode(
    signatory_name, 
    origin = "country.name",
    destination = "iso3c")
    ) %>%
  # remove some NA country codes (small territories)
  filter(!(is.na(signatory_iso))) %>%
  # create quarter column
  mutate(qtr_eif = as.yearqtr(date_eif)) %>%
  select(qtr_eif, fta_id, signatory_iso)

fta_pairs <- fta_dates_long %>%
  inner_join(fta_dates_long, by = c("qtr_eif", "fta_id")) %>%
  # first, keep only pairs where the first country is "less" than the second
  filter(signatory_iso.x < signatory_iso.y) %>%
  transmute(qtr_eif = qtr_eif,
            country_1 = signatory_iso.x,
            country_2 = signatory_iso.y,
            fta = 1)

# now swap the countries in the pair
fta_pairs_swapped <- fta_pairs %>% 
  rename("country_2" = country_1, "country_1" = country_2)

# create combined table with all FTA pairs and quarters of entry into force
fta_pairs_combined <- bind_rows(fta_pairs, fta_pairs_swapped) %>%
  # retain only the earliest date for each country pair
  group_by(country_1, country_2) %>%
  filter(qtr_eif == min(qtr_eif)) %>%
  ungroup()

# create sequence of quarters from 1960 q2 (earliest fta in data) to 2024 q4
quarters <- seq(as.yearqtr("1960 Q2", format = "%Y Q%q"),
                as.yearqtr("2024 Q4", format = "%Y Q%q"),
                by = 0.25)
quarters_df <- tibble(year_qtr = quarters)

# first ensure uniqueness
fta_pairs_unique <- fta_pairs_combined %>% distinct(country_1, country_2, qtr_eif)

# expand grid and mark quarters on or after entry into force as fta = 1
fta_expanded <- fta_pairs_unique %>%
  crossing(quarters_df) %>%  # creates every combination of country pair and quarter
  mutate(fta = if_else(year_qtr >= qtr_eif, 1, 0)) %>%
  # we only need fta = 1 (as we will merge with larger dataset later)
  filter(fta == 1) %>% 
  select(year_qtr, country_1, country_2, fta)

write_parquet(fta_expanded, "data/processed/wto_fta_tidy.parquet")
