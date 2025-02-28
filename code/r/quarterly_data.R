
# combine sources into quarterly data set for regressions

# install V-Dem package from GitHub
# devtools::install_github("vdeminstitute/vdemdata")

library(arrow)
library(countrycode)
library(tidyverse)
library(vdemdata)
library(zoo)


# trade agreements --------------------------------------------------------

# import and tidy trade agreement data

ftas_raw <- read_parquet("data/processed/wto_fta_tidy.parquet")

ftas <- ftas_raw %>%
  rename("iso_o" = country_1, "iso_d" = country_2) %>%
  select(iso_o, iso_d, year_qtr, fta) %>%
  distinct()

remove(ftas_raw)


# GDELT -------------------------------------------------------------------

# import GDELT data and create annual index

gdelt_raw <- read_parquet("data/processed/gdelt_filtered.parquet")

# note country_1 is earlier in alphabet than country_2; pairs aren't duplicated
gdelt_left <- gdelt_raw %>% 
  mutate(year_qtr = as.yearqtr(month_year)) %>%
  group_by(country_1, country_2, year_qtr) %>%
  summarise(gsf_adj = sum(filtered_index)) %>%
  rename("iso_o" = country_1, "iso_d" = country_2)

gdelt_right <- gdelt_left %>% rename("iso_o" = iso_d, "iso_d" = iso_o)

gdelt <- bind_rows(gdelt_left, gdelt_right)
remove(gdelt_raw, gdelt_left, gdelt_right)


# CEPII gravity data ------------------------------------------------------

# load CEPII gravity dataset for grav variables, WTO/GATT/EU membership
gravity_raw <- read_rds("data/raw/Gravity_V202211.rds")

# time-invariant gravity variables
gravity_ti <- gravity_raw %>% 
  select(iso3_o, iso3_d, dist, comcol, comlang_off) %>%
  group_by(iso3_o, iso3_d) %>%
  summarise(dist = mean(dist, na.rm = TRUE), 
            comcol = last(comcol),
            comlang_off = last(comlang_off)) %>%
  drop_na()

# time-varying gravity variables
gravity_tv <- gravity_raw %>% 
  select(iso3_o, iso3_d, year, gatt_o, gatt_d, wto_o, wto_d, eu_o, eu_d) %>%
  drop_na()

# update for 2022-2024: in most cases, this is just repeating 2021
gravity_2021 <- gravity_tv %>% filter(year == 2021)

gravity_2022_2024 <- gravity_2021 %>%
  select(-year) %>%
  crossing(tibble(year = 2022:2024)) %>%
  select(iso3_o, iso3_d, year, gatt_o, gatt_d, wto_o, wto_d, eu_o, eu_d)

gravity_tv_updated <- bind_rows(gravity_tv, gravity_2022_2024) %>%
  arrange(iso3_o, iso3_d, year) %>%
  # update for comoros, timor-leste joining WTO
  mutate(
    wto_o = ifelse(iso3_o %in% c("COM", "TLS") & year == 2024, 1, wto_o),
    wto_d = ifelse(iso3_d %in% c("COM", "TLS") & year == 2024, 1, wto_d)
  ) %>%
  # GATT variable should = 1 if WTO member
  mutate(
    gatt_o = ifelse(wto_o == 1, 1, gatt_o),
    gatt_d = ifelse(wto_d == 1, 1, gatt_d)
  )

# combine time-varying and time-invariant gravity tables
gravity_full <- gravity_tv_updated %>%
  left_join(gravity_ti, by = c("iso3_o", "iso3_d")) %>%
  rename("iso_o" = iso3_o, "iso_d" = iso3_d)

# convert to quarterly
gravity_quarterly <- gravity_full %>%
  # replicate each row four times (one for each quarter)
  crossing(qtr = 1:4) %>%
  # create a zoo::yearqtr column 
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>% 
  select(-c(qtr, year))

remove(gravity_raw, gravity_ti, gravity_tv, gravity_2021, gravity_2022_2024,
       gravity_tv_updated, gravity_full)


# international trade -----------------------------------------------------

# load in import data
imports_raw <- read_csv("data/raw/imf_imports_quarterly.csv")

imports <- imports_raw %>%
  mutate(year_qtr = as.yearqtr(time_period),
         iso_o = countrycode(
           counterpart_area,
           origin = "iso2c",
           destination = "iso3c"
         ),
         iso_d = countrycode(
           reporter,
           origin = "iso2c",
           destination = "iso3c"
         ),
         trade_flow = value * 1000000) %>% # convert from millions USD
  select(iso_o, iso_d, year_qtr, trade_flow) %>%
  drop_na()

remove(imports_raw)


# estimate intra-national trade -------------------------------------------

# load in GDP, exchange rate and export data
gdp_raw <- read_csv("data/raw/imf_gdp_quarterly.csv") # note: in domestic currency
er_raw <- read_csv("data/raw/imf_exchange_quarterly.csv")
exports_raw <- read_csv("data/raw/imf_exports_quarterly.csv")

# total exports
total_exports <- exports_raw %>%
  filter(counterpart_area == "W00") %>%
  mutate(year_qtr = as.yearqtr(time_period),
         iso = countrycode(
           reporter, 
           origin = "iso2c",
           destination = "iso3c"
         ),
         total_exports = value * 1000000) %>% # convert from millions USD
  select(iso, year_qtr, total_exports) %>%
  drop_na()

# estimate USD GDP
gdp <- gdp_raw %>%
  mutate(gdp_dc = value * 1000000) %>% # GDP in millions of domestic currency
  select(reporter, time_period, gdp_dc) %>%
  left_join(er_raw, by = c("time_period", "reporter")) %>%
  mutate(gdp_usd = gdp_dc * value) %>%
  mutate(iso = countrycode(
    reporter,
    origin = "iso2c",
    destination = "iso3c"
  ), 
  year_qtr = as.yearqtr(time_period)) %>%
  select(iso, year_qtr, gdp_usd) %>%
  drop_na()

# estimate intra-national trade
intra <- gdp %>%
  left_join(total_exports, by = c("iso", "year_qtr")) %>%
  mutate(trade_flow = gdp_usd - total_exports,
         iso_o = iso,
         iso_d = iso) %>%
  filter(trade_flow > 0) %>% # drop negative estimates (see campos et al. 2021)
  select(iso_o, iso_d, year_qtr, trade_flow) %>%
  # remove sierra leone and myanmar due to exchange rate issues
  filter(!(iso_o %in% c("SLE", "MMR")))

remove(gdp_raw, gdp, er_raw, exports_raw, total_exports)


# V-Dem data --------------------------------------------------------------

# make sure vdem package is loaded

vdem_data <- vdem %>%
  # select 1) judicial constraints on executive, 2) legislative constraints on
  # executive, 3) rule of law, 4) political corruption, 5) liberal democracy
  # can change these later if need be
  select(country_name, year, v2x_jucon, v2xlg_legcon, v2x_rule, v2x_corr, v2x_libdem) %>%
  mutate(iso = countrycode(
    country_name,
    origin = "country.name",
    destination = "iso3c"
  )) %>%
  drop_na()

vdem_o <- vdem_data %>%
  select(iso, year, v2x_jucon, v2xlg_legcon, v2x_rule, v2x_corr, v2x_libdem) %>%
  rename(
    "iso_o" = iso,
    "v2x_jucon_o" = v2x_jucon,
    "v2xlg_legcon_o" = v2xlg_legcon,
    "v2x_rule_o" = v2x_rule,
    "v2x_corr_o" = v2x_corr,
    "v2x_libdem_o" = v2x_libdem
  ) %>%
  # replicate each row four times (one for each quarter)
  crossing(qtr = 1:4) %>%
  # create a zoo::yearqtr column 
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>% 
  select(iso_o, year_qtr, v2x_jucon_o, v2xlg_legcon_o, v2x_rule_o, v2x_corr_o, v2x_libdem_o) %>%
  distinct()

vdem_d <- vdem_o %>% 
  rename(
    "iso_d" = iso_o,
    "v2x_jucon_d" = v2x_jucon_o,
    "v2xlg_legcon_d" = v2xlg_legcon_o,
    "v2x_rule_d" = v2x_rule_o,
    "v2x_corr_d" = v2x_corr_o,
    "v2x_libdem_d" = v2x_libdem_o
  )

remove(vdem_data)


# combine all data sources ------------------------------------------------

quarterly_data <- bind_rows(imports, intra) %>%
  left_join(ftas, by = c("iso_o", "iso_d", "year_qtr")) %>%
  left_join(gravity_quarterly, by = c("iso_o", "iso_d", "year_qtr")) %>%
  left_join(gdelt, by = c("iso_o", "iso_d", "year_qtr")) %>%
  left_join(vdem_d, by = c("iso_d", "year_qtr")) %>%
  left_join(vdem_o, by = c("iso_o", "year_qtr")) %>%
  # the only NA GDELT indexes should be between a country and itself
  filter(!((is.na(gsf_adj) & (iso_o != iso_d)))) %>%
  mutate(
    fta = replace_na(fta, 0),
    iso_o = as.character(iso_o),
    iso_d = as.character(iso_d),
    gatt_both = gatt_o * gatt_d,
    wto_both = wto_o * wto_d,
    eu_both = eu_o * eu_d,
    pair = paste(iso_o, iso_d, sep = "-"),
    border = if_else(iso_o != iso_d, 1, 0),
    year_qtr_o = paste(iso_o, year_qtr, sep = "-"),
    year_qtr_d = paste(iso_d, year_qtr, sep = "-"),
    border_year_qtr = if_else(border == 1, as.character(year_qtr), "0")
  ) %>%
  mutate(
    gatt_one = if_else((gatt_o + gatt_d == 1) & (border == 1),
                       1,
                       0),
    gatt_both = gatt_o * gatt_d * border,
    wto_one = if_else((wto_o + wto_d == 1) & (border == 1),
                      1,
                      0),
    wto_both = wto_o * wto_d * border,
    eu_one = if_else((eu_o + eu_d == 1) & (border == 1),
                     1,
                     0),
    eu_both = eu_o * eu_d * border
  ) %>%
  arrange(iso_o, iso_d, year_qtr)

# save data

write_parquet(quarterly_data, "data/processed/quarterly_data.parquet")
