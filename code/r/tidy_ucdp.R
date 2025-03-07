
# load required packages
library(countrycode)
library(stringr)
library(tidyr)

# load UCDP dyadic dataset
data <- read_rds("data/raw/ucdp-dyadic-241.rds")

# get set of country pairs at war in given year
war_pairs <- data %>% 
  # split strings (e.g. convert "200, 201" to 200 and 201 in separate rows)
  mutate(gwno_a = str_split(gwno_a, ",\\s*")) %>%
  unnest(gwno_a) %>%
  mutate(gwno_a = as.numeric(gwno_a)) %>%
  mutate(gwno_b = str_split(gwno_b, ",\\s*")) %>%
  unnest(gwno_b) %>%
  mutate(gwno_b = as.numeric(gwno_b)) %>%
  # convert gleditsch & ward identifiers to iso codes
  mutate(
    iso_a = countrycode(
      gwno_a,
      origin = "gwn",
      destination = "iso3c"
      ),
    iso_b = countrycode(
      gwno_b,
      origin = "gwn",
      destination = "iso3c"
      )
  ) %>%
  select(iso_a, iso_b, year) %>%
  drop_na(iso_a, iso_b) %>%
  distinct()

# get set of countries where wars are occurring in given year
war_locs <- data %>% 
  mutate(gwno_loc = str_split(gwno_loc, ",\\s*")) %>%
  unnest(gwno_loc) %>%
  mutate(gwno_loc = as.numeric(gwno_loc)) %>%
  mutate(
    iso_loc = countrycode(
      gwno_loc,
      origin = "gwn",
      destination = "iso3c")
    ) %>%
  select(iso_loc, year) %>%
  drop_na(iso_loc) %>%
  distinct()

# save data
write_csv(war_pairs, "data/temp/war_pairs.csv")
write_csv(war_locs, "data/temp/war_locs.csv")

