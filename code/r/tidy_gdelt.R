
library(arrow) # to save as parquet
library(tidyverse)

# load data from bigquery output, restrict to 1980-2024
gdelt_bq <- read_csv("data/raw/bigquery_output.csv") %>%
  filter(month_year %in% c(198000:202412))

# make symmetrical
gdelt_sym <- gdelt_bq %>%
  mutate(
    country_low = pmin(country_1, country_2),
    country_high = pmax(country_1, country_2)
  ) %>%
  group_by(month_year, country_low, country_high) %>%
  summarise(
    goldstein_sum = sum(goldstein_sum, na.rm = TRUE),
    num_events = sum(num_events, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(country_1 = country_low, country_2 = country_high)

# add total event numbers (e.g. all US events in month) to weight index
total_events <- gdelt_sym %>%
  pivot_longer(
    cols = c(country_1, country_2),
    names_to = "position",
    values_to = "country"
  ) %>%
  group_by(month_year, country) %>%
  summarise(total_events = sum(num_events, na.rm = TRUE), .groups = "drop")

# join symmetrical data with total events to make new main dataframe, events
events <- gdelt_sym %>%
  left_join(total_events, by = c("month_year", "country_1" = "country")) %>%
  rename(n_1 = total_events) %>%
  left_join(total_events, by = c("month_year", "country_2" = "country")) %>%
  rename(n_2 = total_events) %>%
  # calculate adjusted goldstein index
  mutate(gs_adj = goldstein_sum / (n_1 + n_2 - num_events))
remove(gdelt_sym, total_events)

# fill in missing zeroes
all_months <- unique(events$month_year) %>% sort()
countries <- unique(c(events$country_1, events$country_2)) %>% sort()

full_grid <- expand_grid(
  month_year = all_months,
  country_1 = countries, 
  country_2 = countries
) %>%
  filter(country_1 < country_2) # avoid duplicates and self pairs

gdelt_full <- full_grid %>%
  left_join(events, by = c("month_year", "country_1", "country_2")) %>%
  replace(is.na(.), 0) %>%
  arrange(month_year, country_1, country_2)

# save tidied data
write_parquet(gdelt_full, "data/temp/gdelt_tidy.parquet")
