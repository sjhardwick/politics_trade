
# combine sources into quarterly data set for regressions

# install V-Dem package from GitHub
# devtools::install_github("vdeminstitute/vdemdata")

library(arrow)
library(countrycode)
library(lubridate)
library(readxl)
library(tidyverse)
library(vdemdata)
library(zoo)


# trade agreements --------------------------------------------------------

# import and tidy trade agreement data

ftas_raw <- read_parquet("data/temp/wto_fta_tidy.parquet")

ftas <- ftas_raw %>%
  rename("iso_o" = country_1, "iso_d" = country_2) %>%
  select(iso_o, iso_d, year_qtr, fta) %>%
  rename("rta" = fta) %>% # to be technically correct
  distinct(iso_o, iso_d, year_qtr, .keep_all = TRUE)

remove(ftas_raw)


# GDELT -------------------------------------------------------------------

# import GDELT data and create annual index

# gdelt_full is gdelt data for all available countries, including those with
# insufficient observations for filtering
gdelt_full <- read_parquet("data/temp/gdelt_tidy.parquet")

gdelt_full_tidy <- gdelt_full %>%
  rename("gsa" = gs_adj) %>%
  select(country_1, country_2, month_year, gsa)

# gdelt_filter contains the filtered GDELT indexes
gdelt_filter <- read_parquet("data/temp/gdelt_filtered.parquet") %>%
  select(-gsa) # ignore this column; it's in gdelt_full_tidy

# note country_1 is earlier in alphabet than country_2; pairs aren't duplicated
gdelt_left <- gdelt_full_tidy %>%
  left_join(gdelt_filter, by = c("country_1", "country_2", "month_year")) %>%
  mutate(month_year = ym(month_year)) %>%
  mutate(year_qtr = as.yearqtr(month_year)) %>%
  group_by(country_1, country_2, year_qtr) %>%
  summarise(gsa = sum(gsa),
            gsaf = sum(gsaf),
            gsaf_baseline = sum(gsaf_baseline),
            gsaf_large = sum(gsaf_large),
            gsaf_small = sum(gsaf_small)) %>%
  rename("iso_o" = country_1, "iso_d" = country_2)

gdelt_right <- gdelt_left %>% rename("iso_o" = iso_d, "iso_d" = iso_o)

gdelt <- bind_rows(gdelt_left, gdelt_right) %>%
  distinct(iso_o, iso_d, year_qtr, .keep_all = TRUE)

remove(gdelt_left, gdelt_right, gdelt_filter, gdelt_full, gdelt_full_tidy)
gc()


# CEPII gravity data ------------------------------------------------------

# load CEPII gravity dataset for grav variables, WTO/GATT/EU membership
gravity_raw <- read_rds("data/raw/Gravity_V202211.rds")

# time-invariant gravity variables
gravity_ti <- gravity_raw %>% 
  select(iso3_o, iso3_d, dist, contig, comcol, comlang_off) %>%
  group_by(iso3_o, iso3_d) %>%
  summarise(
    dist = mean(dist, na.rm = TRUE), 
    contig = first(contig),
    comcol = last(comcol),
    comlang_off = last(comlang_off)
  ) %>%
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
    wto_o = ifelse(iso3_o %in% c("COM", "TLS") & year >= 2024, 1, wto_o),
    wto_d = ifelse(iso3_d %in% c("COM", "TLS") & year >= 2024, 1, wto_d)
  ) %>%
  # edit GATT variable to = 0 from 1995 onwards, matching larch et al. (2024)
  mutate(
    gatt_o = ifelse(year >= 1995, 0, gatt_o),
    gatt_d = ifelse(year >= 1995, 0, gatt_d)
  ) %>%
  # create 'GATT or WTO' variable
  mutate(
    gatt_wto_o = pmax(gatt_o, wto_o),
    gatt_wto_d = pmax(gatt_d, wto_d)
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
gc()


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
gc()


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
  # remove sierra leone and myanmar due to exchange rate data issues
  filter(!(iso_o %in% c("SLE", "MMR")))

remove(gdp_raw, gdp, er_raw, exports_raw, total_exports)
gc()


# V-Dem data --------------------------------------------------------------

# make sure vdem package is loaded

vdem_data <- vdem %>%
  # select 1) rule of law, 2) political corruption, 3) liberal democracy,
  # 4) polyarchy (a.k.a. electoral democracy)
  # can change these later if need be
  select(country_name, year, v2x_rule, v2x_corr, v2x_libdem, v2x_polyarchy) %>%
  mutate(iso = countrycode(
    country_name,
    origin = "country.name",
    destination = "iso3c"
  )) %>%
  drop_na() %>%
  # PSE includes british mandate (up to 1948), gaza strip and west bank
  # retain just the average of gaza and west bank
  filter(country_name != "Palestine/British Mandate") %>%
  group_by(iso, year) %>%
  summarise(
    v2x_rule = mean(v2x_rule, na.rm = TRUE),
    v2x_corr = mean(v2x_corr, na.rm = TRUE),
    v2x_libdem = mean(v2x_libdem, na.rm = TRUE),
    v2x_polyarchy = mean(v2x_polyarchy, na.rm = TRUE)
  ) %>%
  ungroup()

vdem_o <- vdem_data %>%
  select(iso, year, v2x_rule, v2x_corr, v2x_libdem, v2x_polyarchy) %>%
  rename(
    "iso_o" = iso,
    "v2x_rule_o" = v2x_rule,
    "v2x_corr_o" = v2x_corr,
    "v2x_libdem_o" = v2x_libdem,
    "v2x_polyarchy_o" = v2x_polyarchy
  ) %>%
  # replicate each row four times (one for each quarter)
  crossing(qtr = 1:4) %>%
  # create a zoo::yearqtr column 
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>% 
  select(iso_o, year_qtr, v2x_rule_o, v2x_corr_o, v2x_libdem_o, v2x_polyarchy_o) %>%
  distinct()

vdem_d <- vdem_o %>% 
  rename(
    "iso_d" = iso_o,
    "v2x_rule_d" = v2x_rule_o,
    "v2x_corr_d" = v2x_corr_o,
    "v2x_libdem_d" = v2x_libdem_o,
    "v2x_polyarchy_d" = v2x_polyarchy_o
  )

remove(vdem_data)
gc()


# polity, WGI, QoG --------------------------------------------------------

polity_raw <- read_csv("data/raw/polity5_2018.csv")
wgi_raw <- read_excel("data/raw/wgidataset.xlsx")
qog_raw <- read_csv("data/raw/qog_std_ts_jan25.csv")

# tidy polity 5 data
polity_o <- polity_raw %>%
  mutate(iso_o = countrycode(scode,
                             origin = "cowc",
                             destination = "iso3c"),
         polity_o = polity2) %>%
  # remove flags/missing values
  filter(polity_o >= -10, polity_o <= 10) %>%
  drop_na(iso_o) %>%
  select(year, iso_o, polity_o) %>%
  # replicate each row four times (one for each quarter)
  crossing(qtr = 1:4) %>%
  # create a zoo::yearqtr column 
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>%
  select(year_qtr, iso_o, polity_o)

polity_d <- polity_o %>% rename("iso_d" = iso_o, "polity_d" = polity_o)

# tidy worldwide governance indicators (WGI) data
wgi_o <- wgi_raw %>%
  select(countryname, year, indicator, estimate) %>%
  # NAs okay here (they are missing values)
  mutate(estimate = as.numeric(estimate)) %>%
  pivot_wider(names_from = indicator, 
              values_from = estimate) %>%
  mutate(iso_o = countrycode(countryname,
                             origin = "country.name",
                             destination = "iso3c")) %>%
  drop_na(iso_o) %>%
  rename("cc_o" = cc, 
         "ge_o" = ge, 
         "pv_o" = pv, 
         "rl_o" = rl, 
         "rq_o" = rq, 
         "va_o" = va) %>%
  # replicate each row four times (one for each quarter)
  crossing(qtr = 1:4) %>%
  # create a zoo::yearqtr column 
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>%
  select(-c(countryname, year, qtr))

wgi_d <- wgi_o %>%
  rename("iso_d" = iso_o,
         "cc_d" = cc_o,
         "ge_d" = ge_o,
         "pv_d" = pv_o,
         "rl_d" = rl_o,
         "rq_d" = rq_o,
         "va_d" = va_o)

# get transparency intl corruption perceptions index from QoG data

cpi_o <- qog_raw %>%
  select(year, cname, ti_cpi) %>%
  drop_na(ti_cpi) %>%
  distinct() %>%
  mutate(iso_o = countrycode(cname,
                             origin = "country.name",
                             destination = "iso3c")) %>%
  rename("ti_cpi_o" = ti_cpi) %>%
  # replicate each row four times (one for each quarter)
  crossing(qtr = 1:4) %>%
  # create a zoo::yearqtr column 
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>%
  select(year_qtr, iso_o, ti_cpi_o)

cpi_d <- cpi_o %>% rename("iso_d"= iso_o, "ti_cpi_d" = ti_cpi_o)

remove(polity_raw, wgi_raw, qog_raw)


# war data ----------------------------------------------------------------

# load in data on wars originally from UCDP
war_locs_raw <- read_csv("data/temp/war_locs.csv")
war_pairs_raw <- read_csv("data/temp/war_pairs.csv")

# list of countries where war is occurring by year
war_locs_o <- war_locs_raw %>%
  rename("iso_o" = iso_loc) %>%
  mutate(war_loc_o = 1) %>%
  # replicate each row four times (one for each quarter)
  crossing(qtr = 1:4) %>%
  # create a zoo::yearqtr column
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>%
  select(-c(year, qtr))

war_locs_d <- war_locs_o %>%
  rename("iso_d" = iso_o, "war_loc_d" = war_loc_o)

# list of country pairs at war in given year
war_pairs_a <- war_pairs_raw %>%
  mutate(pair = paste(iso_a, iso_b, sep = "-"),
         war_pair = 1) %>%
  crossing(qtr = 1:4) %>%
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>%
  select(pair, year_qtr, war_pair)

war_pairs_b <- war_pairs_raw %>%
  mutate(pair = paste(iso_b, iso_a, sep = "-"),
         war_pair = 1) %>%
  crossing(qtr = 1:4) %>%
  mutate(year_qtr = as.yearqtr(sprintf("%d-Q%d", year, qtr), format = "%Y-Q%q")) %>%
  select(pair, year_qtr, war_pair)

war_pairs <- bind_rows(war_pairs_a, war_pairs_b)

remove(war_locs_raw, war_pairs_raw, war_pairs_a, war_pairs_b)


# combine all data sources ------------------------------------------------

# define inverse hyperbolic sine transformation function for indexes
ihs_transform <- function(x) {
  ifelse(is.na(x), NA, log(x + sqrt(x^2 + 1)))
}

# merge all data sets
quarterly_data <- bind_rows(imports, intra) %>%
  
  # trade agreements, gravity variables, political distance measures
  left_join(ftas, by = c("iso_o", "iso_d", "year_qtr")) %>%
  left_join(gravity_quarterly, by = c("iso_o", "iso_d", "year_qtr")) %>%
  left_join(gdelt, by = c("iso_o", "iso_d", "year_qtr")) %>%
  
  # non-trade-related institutional indexes; wars by country
  left_join(vdem_d, by = c("iso_d", "year_qtr")) %>%
  left_join(vdem_o, by = c("iso_o", "year_qtr")) %>%
  left_join(polity_d, by = c("iso_d", "year_qtr")) %>%
  left_join(polity_o, by = c("iso_o", "year_qtr")) %>%
  left_join(wgi_d, by = c("iso_d", "year_qtr")) %>%
  left_join(wgi_o, by = c("iso_o", "year_qtr")) %>%
  left_join(cpi_d, by = c("iso_d", "year_qtr")) %>%
  left_join(cpi_o, by = c("iso_o", "year_qtr")) %>%
  left_join(war_locs_d, by = c("iso_d", "year_qtr")) %>%
  left_join(war_locs_o, by = c("iso_o", "year_qtr")) %>%
  
  # revise/define additional variables
  mutate(
    rta = replace_na(rta, 0),
    
    # create various IDs for panel structure and fixed effects
    iso_o = as.character(iso_o),
    iso_d = as.character(iso_d),
    pair = paste(iso_o, iso_d, sep = "-"),
    border = as.integer(iso_o != iso_d),
    year_qtr_o = paste(iso_o, year_qtr, sep = "-"),
    year_qtr_d = paste(iso_d, year_qtr, sep = "-"),
    border_year_qtr = if_else(border == 1, as.character(year_qtr), "0")
  ) %>%
  
  # add war pair indicator and replace NA with zero
  left_join(war_pairs, by = c("year_qtr", "pair")) %>%
  mutate(
    war_pair = replace_na(war_pair, 0),
    war_loc_o = replace_na(war_loc_o, 0),
    war_loc_d = replace_na(war_loc_d, 0)
  ) %>%
  
  # create additional border-dependent WTO, GATT, EU variables
  mutate(
    gatt_one = as.integer((gatt_o + gatt_d == 1) & (border == 1)),
    gatt_both = gatt_o * gatt_d * border,
    wto_one = as.integer((wto_o + wto_d == 1) & (border == 1)),
    wto_both = wto_o * wto_d * border,
    gatt_wto_one = as.integer((gatt_wto_o + gatt_wto_d == 1) & (border == 1)),
    gatt_wto_both = gatt_wto_o * gatt_wto_d * border,
    eu_one = as.integer((eu_o + eu_d == 1) & (border == 1)),
    eu_both = eu_o * eu_d * border
  ) %>%
  
  # create goldstein-based indexes for regressions using IHS transformation
  mutate(
    across(c(gsa, gsaf, gsaf_baseline, gsaf_large, gsaf_small), 
           ~ case_when(border == 1 ~ ihs_transform(-.x), 
                       border == 0 ~ 0),
           .names = "ihs_{.col}")
  ) %>%
  
  # scale polity data to ensure positive values and create democracy indicator
  mutate(polity_scaled_o = (polity_o + 10) / 20,
         polity_scaled_d = (polity_d + 10) / 20,
         polity_dem_o = as.integer(polity_o >= 6),
         polity_dem_d = as.integer(polity_d >= 6)) %>%
  
  # remove rows missing key independent variable (goldstein-based index)
  filter(!is.na(ihs_gsa)) %>%
  
  # sort
  arrange(iso_o, iso_d, year_qtr)

# check uniqueness
duplicate_rows <- quarterly_data %>%
  count(iso_o, iso_d, year_qtr) %>%
  filter(n > 1) %>%
  nrow()

if (duplicate_rows > 0) {
  warning(paste("Warning:", duplicate_rows, "duplicate rows found!"))
}

# save data
write_parquet(quarterly_data, "data/processed/quarterly_data.parquet")
