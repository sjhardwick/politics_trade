
# combine sources into annual data set for regressions

# install V-Dem package from GitHub
# devtools::install_github("vdeminstitute/vdemdata")

library(arrow)
library(countrycode)
library(readxl)
library(tidyverse)
library(vdemdata)
library(zoo)


# UNGA ideal point estimates ----------------------------------------------

# import and tidy UNGA voting ideal point distances
ipds_raw <- read_csv("data/raw/IdealpointestimatesAll_Jun2024.csv")

ipds <- ipds_raw %>%
  mutate(year = session + 1945, 
         iso = iso3c,
         ipe = IdealPointAll) %>%
  select(iso, year, ipe)

# ideal point estimates for importer (o) and exporter (d)
ipd_o <- ipds %>% rename("iso_o" = iso, "ipe_o" = ipe)
ipd_d <- ipds %>% rename("iso_d" = iso, "ipe_d" = ipe)

remove(ipds_raw, ipds)


# trade agreements --------------------------------------------------------

# import and tidy trade agreement data

ftas_raw <- read_parquet("data/processed/wto_fta_tidy.parquet")

ftas <- ftas_raw %>%
  rename("iso_o" = country_1, "iso_d" = country_2) %>%
  mutate(year = as.numeric(str_extract(year_qtr, "^\\d{4}"))) %>%
  select(iso_o, iso_d, year, fta) %>%
  rename("rta" = fta) %>% # to be technically correct
  distinct(iso_o, iso_d, year, .keep_all = TRUE)

remove(ftas_raw)


# GDELT -------------------------------------------------------------------

# import GDELT data and create annual index

gdelt_raw <- read_parquet("data/processed/gdelt_filtered.parquet")

# note country_1 is earlier in alphabet than country_2; pairs aren't duplicated
gdelt_left <- gdelt_raw %>% 
  mutate(year = year(month_year)) %>%
  group_by(country_1, country_2, year) %>%
  summarise(gsf_adj = sum(filtered_index)) %>%
  rename("iso_o" = country_1, "iso_d" = country_2)

gdelt_right <- gdelt_left %>% rename("iso_o" = iso_d, "iso_d" = iso_o)

gdelt <- bind_rows(gdelt_left, gdelt_right) %>%
  distinct(iso_o, iso_d, year, .keep_all = TRUE)

remove(gdelt_raw, gdelt_left, gdelt_right)


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
    comcol = first(comcol),
    comlang_off = first(comlang_off)
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

remove(gravity_raw, gravity_ti, gravity_tv, gravity_2021, gravity_2022_2024,
       gravity_tv_updated)


# international trade -----------------------------------------------------

# load in import data
imports_raw <- read_csv("data/raw/imf_imports_annual.csv")

imports <- imports_raw %>%
  mutate(year = year(time_period),
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
  select(iso_o, iso_d, year, trade_flow) %>%
  drop_na()

remove(imports_raw)


# estimate intra-national trade -------------------------------------------

# load in GDP, exchange rate and export data
gdp_raw <- read_csv("data/raw/imf_gdp_annual.csv") # note: in domestic currency
er_raw <- read_csv("data/raw/imf_exchange_annual.csv")
exports_raw <- read_csv("data/raw/imf_exports_annual.csv")

# total exports
total_exports <- exports_raw %>%
  filter(counterpart_area == "W00") %>%
  mutate(year = year(time_period),
         iso = countrycode(
           reporter, 
           origin = "iso2c",
           destination = "iso3c"
           ),
         total_exports = value * 1000000) %>% # convert from millions USD
  select(iso, year, total_exports) %>%
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
         year = year(time_period)) %>%
  select(iso, year, gdp_usd) %>%
  drop_na()

# estimate intra-national trade
intra <- gdp %>%
  left_join(total_exports, by = c("iso", "year")) %>%
  mutate(trade_flow = gdp_usd - total_exports,
         iso_o = iso,
         iso_d = iso) %>%
  filter(trade_flow > 0) %>% # drop negative estimates (see campos et al. 2021)
  select(iso_o, iso_d, year, trade_flow) %>%
  # remove sierra leone and myanmar due to exchange rate data issues
  filter(!(iso_o %in% c("SLE", "MMR")))

remove(gdp_raw, gdp, er_raw, exports_raw, total_exports)


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
  )

vdem_d <- vdem_o %>% 
  rename(
    "iso_d" = iso_o,
    "v2x_rule_d" = v2x_rule_o,
    "v2x_corr_d" = v2x_corr_o,
    "v2x_libdem_d" = v2x_libdem_o,
    "v2x_polyarchy_d" = v2x_polyarchy_o
    )

remove(vdem_data)


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
  select(year, iso_o, polity_o)

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
  select(-countryname)

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
  select(-cname)

cpi_d <- cpi_o %>%
  rename("iso_d"= iso_o, "ti_cpi_d" = ti_cpi_o)

remove(polity_raw, wgi_raw, qog_raw)


# combine all data sources ------------------------------------------------

annual_data <- bind_rows(imports, intra) %>%
  left_join(ftas, by = c("iso_o", "iso_d", "year")) %>%
  left_join(gravity_full, by = c("iso_o", "iso_d", "year")) %>%
  left_join(gdelt, by = c("iso_o", "iso_d", "year")) %>%
  left_join(ipd_d, by = c("iso_d", "year")) %>%
  left_join(ipd_o, by = c("iso_o", "year")) %>%
  left_join(vdem_d, by = c("iso_d", "year")) %>%
  left_join(vdem_o, by = c("iso_o", "year")) %>%
  left_join(polity_d, by = c("iso_d", "year")) %>%
  left_join(polity_o, by = c("iso_o", "year")) %>%
  left_join(wgi_d, by = c("iso_d", "year")) %>%
  left_join(wgi_o, by = c("iso_o", "year")) %>%
  left_join(cpi_d, by = c("iso_d", "year")) %>%
  left_join(cpi_o, by = c("iso_o", "year")) %>%
  mutate(
    rta = replace_na(rta, 0),
    ipd = abs(ipe_d - ipe_o),
    iso_o = as.character(iso_o),
    iso_d = as.character(iso_d),
    year = as.integer(year),
    pair = paste(iso_o, iso_d, sep = "-"),
    border = if_else(iso_o != iso_d, 1, 0),
    year_o = paste(iso_o, year, sep = "-"),
    year_d = paste(iso_d, year, sep = "-"),
    border_year = if_else(border == 1, year, 0)
  ) %>%
  # factors more memory-efficient for fixed effects
  mutate(
    pair = as.factor(pair),
    year_o = as.factor(year_o),
    year_d = as.factor(year_d)
  ) %>%
  mutate(
    # ideal point distance (IPD) = border * IPD
    ipd = if_else(border == 1, ipd, 0),
    # create goldstein-based index for regressions
    # use inverse hyperbolic sine to log-transform while preserving order
    ihs_gsf = case_when(border == 1 ~ log(-gsf_adj + sqrt((-gsf_adj)^2 + 1)),
                        TRUE ~ 0),
    gatt_one = if_else((gatt_o + gatt_d == 1) & (border == 1),
                       1,
                       0),
    gatt_both = gatt_o * gatt_d * border,
    wto_one = if_else((wto_o + wto_d == 1) & (border == 1),
                      1,
                      0),
    wto_both = wto_o * wto_d * border,
    gatt_wto_one = if_else((gatt_wto_o + gatt_wto_d == 1) & (border == 1),
                      1,
                      0),
    gatt_wto_both = gatt_wto_o * gatt_wto_d * border,
    eu_one = if_else((eu_o + eu_d == 1) & (border == 1),
                     1,
                     0),
    eu_both = eu_o * eu_d * border
  ) %>%
  # scale polity data to exceed zero
  mutate(polity_o_scaled = (polity_o + 10) / 20,
         polity_d_scaled = (polity_d + 10) / 20) %>%
  # remove if missing both key independent variables
  filter(!((is.na(ihs_gsf)) & (is.na(ipd)))) %>%
  arrange(iso_o, iso_d, year)

# check uniqueness
annual_data %>%
  count(iso_o, iso_d, year) %>%
  filter(n > 1) %>%
  nrow()

# save data

write_parquet(annual_data, "data/processed/annual_data.parquet")
