-- ================================================================
-- COMPLETE DATABASE SCHEMA FOR AI DATACENTER IMPACT DASHBOARD
-- ================================================================
-- Run this script in PostgreSQL to set up all required tables
-- ================================================================

-- Drop existing tables if they exist (for clean rebuild)
DROP TABLE IF EXISTS fact_electricity_prices CASCADE;
DROP TABLE IF EXISTS fact_energy_consumption CASCADE;
DROP TABLE IF EXISTS fact_grid_impact CASCADE;
DROP TABLE IF EXISTS fact_ai_metrics CASCADE;
DROP TABLE IF EXISTS dim_datacenters CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_energy_regions CASCADE;
DROP TABLE IF EXISTS state_subsidies CASCADE;
DROP TABLE IF EXISTS subsidy_timeline CASCADE;
DROP TABLE IF EXISTS dc_consumption_projections CASCADE;
DROP TABLE IF EXISTS virginia_metrics CASCADE;
DROP TABLE IF EXISTS price_projections CASCADE;
DROP TABLE IF EXISTS public_health_impact CASCADE;

-- ================================================================
-- DIMENSION TABLES
-- ================================================================

-- Date Dimension (CRITICAL - needed for time intelligence)
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER NOT NULL CHECK (year BETWEEN 2000 AND 2100),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name VARCHAR(20),
    week INTEGER CHECK (week BETWEEN 1 AND 53),
    day_of_week INTEGER CHECK (day_of_week BETWEEN 1 AND 7),
    is_weekend BOOLEAN DEFAULT FALSE
);

-- Datacenters Dimension
CREATE TABLE dim_datacenters (
    datacenter_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    company VARCHAR(100) NOT NULL,
    location_city VARCHAR(100),
    location_state CHAR(2) NOT NULL,
    latitude DECIMAL(10,8) CHECK (latitude BETWEEN 24.0 AND 71.0),
    longitude DECIMAL(11,8) CHECK (longitude BETWEEN -180.0 AND -66.0),
    capacity_mw DECIMAL(10,2) CHECK (capacity_mw >= 0),
    is_ai_focused BOOLEAN DEFAULT FALSE,
    opening_date DATE,
    renewable_energy_pct DECIMAL(5,2) CHECK (renewable_energy_pct BETWEEN 0 AND 100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- U.S. Energy Regions
CREATE TABLE dim_energy_regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL UNIQUE,
    region_type VARCHAR(20) CHECK (region_type IN ('state', 'iso_rto')),
    grid_operator VARCHAR(100),
    description TEXT
);

-- ================================================================
-- FACT TABLES
-- ================================================================

-- Electricity Prices Fact Table (YOUR EIA DATA)
CREATE TABLE fact_electricity_prices (
    price_id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    date_id INTEGER NOT NULL,
    price_per_kwh DECIMAL(10,4) CHECK (price_per_kwh >= 0),
    price_cents_per_kwh DECIMAL(10,4) CHECK (price_cents_per_kwh >= 0),
    sales_mwh DECIMAL(15,2),
    price_type VARCHAR(50) DEFAULT 'average',
    sector VARCHAR(50),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE CASCADE
);

-- Energy Consumption Fact Table
CREATE TABLE fact_energy_consumption (
    consumption_id SERIAL PRIMARY KEY,
    datacenter_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    energy_consumed_mwh DECIMAL(15,2) CHECK (energy_consumed_mwh >= 0),
    renewable_energy_mwh DECIMAL(15,2) CHECK (renewable_energy_mwh >= 0),
    pue_ratio DECIMAL(4,2) CHECK (pue_ratio >= 1.0),
    source VARCHAR(100),
    FOREIGN KEY (datacenter_id) REFERENCES dim_datacenters(datacenter_id) ON DELETE CASCADE,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE CASCADE
);

-- ================================================================
-- SUPPLEMENTAL TABLES FOR DASHBOARD
-- ================================================================

-- State Subsidies (from Good Jobs First)
CREATE TABLE state_subsidies (
    state CHAR(2) PRIMARY KEY,
    state_name VARCHAR(50) NOT NULL,
    annual_subsidy_millions DECIMAL(10,2),
    year_reported INTEGER,
    subsidy_2020_millions DECIMAL(10,2),
    subsidy_2024_millions DECIMAL(10,2),
    pct_change_2020_2024 DECIMAL(10,2),
    roi_per_dollar DECIMAL(5,2),
    has_dc_in_dataset BOOLEAN DEFAULT FALSE,
    source VARCHAR(255)
);

-- Subsidy Timeline (for growth charts)
CREATE TABLE subsidy_timeline (
    id SERIAL PRIMARY KEY,
    state CHAR(2) NOT NULL,
    year INTEGER NOT NULL,
    subsidy_millions DECIMAL(10,2) NOT NULL,
    source VARCHAR(255)
);

-- DC Consumption Projections (from IEA)
CREATE TABLE dc_consumption_projections (
    year INTEGER PRIMARY KEY,
    us_dc_consumption_twh DECIMAL(10,2),
    us_dc_pct_of_total DECIMAL(5,2),
    global_dc_consumption_twh DECIMAL(10,2),
    data_type VARCHAR(20) CHECK (data_type IN ('actual', 'projection')),
    source VARCHAR(255)
);

-- Virginia Metrics (from JLARC/Dominion)
CREATE TABLE virginia_metrics (
    metric VARCHAR(100) PRIMARY KEY,
    value DECIMAL(15,2),
    unit VARCHAR(50),
    year INTEGER,
    source VARCHAR(255),
    notes TEXT
);

-- Price Projections (for 2025-2030 overlay)
CREATE TABLE price_projections (
    year INTEGER PRIMARY KEY,
    national_avg_price_cents DECIMAL(10,2),
    national_projected_increase_pct DECIMAL(10,2),
    virginia_projected_increase_pct DECIMAL(10,2),
    data_type VARCHAR(20) CHECK (data_type IN ('actual', 'projection')),
    source VARCHAR(255)
);

-- Public Health Impact (from Caltech/UCR)
CREATE TABLE public_health_impact (
    metric VARCHAR(100) PRIMARY KEY,
    value_2023 DECIMAL(15,2),
    value_2030_projected DECIMAL(15,2),
    unit VARCHAR(50),
    source VARCHAR(255),
    notes TEXT
);

-- ================================================================
-- INDEXES FOR PERFORMANCE
-- ================================================================

CREATE INDEX idx_electricity_prices_date ON fact_electricity_prices(date_id);
CREATE INDEX idx_electricity_prices_region ON fact_electricity_prices(region);
CREATE INDEX idx_energy_consumption_datacenter ON fact_energy_consumption(datacenter_id);
CREATE INDEX idx_energy_consumption_date ON fact_energy_consumption(date_id);
CREATE INDEX idx_datacenters_state ON dim_datacenters(location_state);
CREATE INDEX idx_datacenters_company ON dim_datacenters(company);
CREATE INDEX idx_date_full_date ON dim_date(full_date);
CREATE INDEX idx_date_year_month ON dim_date(year, month);
CREATE INDEX idx_subsidy_timeline_state ON subsidy_timeline(state);
CREATE INDEX idx_subsidy_timeline_year ON subsidy_timeline(year);

-- ================================================================
-- POPULATE DATE DIMENSION (2020-2035)
-- Extended to 2035 to cover all projections
-- ================================================================

INSERT INTO dim_date (full_date, year, quarter, month, month_name, week, day_of_week, is_weekend)
SELECT 
    date_series AS full_date,
    EXTRACT(YEAR FROM date_series) AS year,
    EXTRACT(QUARTER FROM date_series) AS quarter,
    EXTRACT(MONTH FROM date_series) AS month,
    TO_CHAR(date_series, 'Month') AS month_name,
    EXTRACT(WEEK FROM date_series) AS week,
    EXTRACT(ISODOW FROM date_series) AS day_of_week,
    CASE WHEN EXTRACT(ISODOW FROM date_series) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2020-01-01'::date,
    '2035-12-31'::date,
    '1 day'::interval
) AS date_series;

-- ================================================================
-- POPULATE STATE SUBSIDIES
-- Source: Good Jobs First (April 2025), State Budget Documents
-- ================================================================

INSERT INTO state_subsidies (state, state_name, annual_subsidy_millions, year_reported, subsidy_2020_millions, subsidy_2024_millions, pct_change_2020_2024, roi_per_dollar, has_dc_in_dataset, source) VALUES
('TX', 'Texas', 1000, 2025, 130, 1000, 669, NULL, TRUE, 'Texas Comptroller'),
('VA', 'Virginia', 732, 2024, NULL, 732, NULL, -0.52, TRUE, 'VA ACFR / JLARC'),
('IL', 'Illinois', 370, 2024, 10, 370, 3600, NULL, TRUE, 'IL Budget'),
('GA', 'Georgia', 296, 2025, NULL, 296, NULL, NULL, TRUE, 'GA Tax Expenditure Report'),
('IA', 'Iowa', 151, 2020, 151, NULL, NULL, NULL, TRUE, 'IA Tax Expenditure Report'),
('NV', 'Nevada', 100, 2024, NULL, 100, NULL, NULL, TRUE, 'Good Jobs First'),
('OH', 'Ohio', 100, 2024, NULL, 100, NULL, NULL, TRUE, 'Good Jobs First'),
('MN', 'Minnesota', 100, 2024, NULL, 100, NULL, NULL, TRUE, 'Good Jobs First'),
('WA', 'Washington', 100, 2024, NULL, 100, NULL, -0.65, TRUE, 'Good Jobs First'),
('TN', 'Tennessee', 100, 2024, NULL, 100, NULL, NULL, TRUE, 'Good Jobs First'),
('AZ', 'Arizona', 19, 2024, 1.4, 19, 1257, NULL, TRUE, 'Good Jobs First'),
('OR', 'Oregon', 50, 2024, NULL, 50, NULL, NULL, TRUE, 'Estimate'),
('CA', 'California', 75, 2024, NULL, 75, NULL, NULL, TRUE, 'Estimate'),
('NC', 'North Carolina', NULL, NULL, NULL, NULL, NULL, NULL, TRUE, 'Not Disclosed'),
('IN', 'Indiana', NULL, NULL, NULL, NULL, NULL, NULL, FALSE, 'Not Disclosed'),
('UT', 'Utah', NULL, NULL, NULL, NULL, NULL, NULL, TRUE, 'Not Disclosed');

-- ================================================================
-- POPULATE SUBSIDY TIMELINE
-- Source: Good Jobs First, Texas Comptroller
-- ================================================================

INSERT INTO subsidy_timeline (state, year, subsidy_millions, source) VALUES
-- Illinois growth (3,600% increase)
('IL', 2020, 10, 'Good Jobs First'),
('IL', 2021, 51, 'Good Jobs First (interpolated)'),
('IL', 2022, 51, 'Good Jobs First'),
('IL', 2023, 371, 'Good Jobs First'),
('IL', 2024, 370, 'Good Jobs First'),
-- Texas historical + projections
('TX', 2023, 157, 'Texas Comptroller (projection)'),
('TX', 2024, 500, 'Texas Comptroller (revised)'),
('TX', 2025, 1000, 'Texas Comptroller'),
('TX', 2026, 1200, 'Texas Comptroller (projection)'),
('TX', 2027, 1400, 'Texas Comptroller (projection)'),
('TX', 2028, 1550, 'Texas Comptroller (projection)'),
('TX', 2029, 1650, 'Texas Comptroller (projection)'),
('TX', 2030, 1700, 'Texas Comptroller (projection)'),
-- Virginia
('VA', 2022, 411, 'JLARC'),
('VA', 2023, 750, 'VA ACFR'),
('VA', 2024, 732, 'VA ACFR');

-- ================================================================
-- POPULATE DC CONSUMPTION PROJECTIONS
-- Source: IEA Energy and AI Report (April 2025)
-- ================================================================

INSERT INTO dc_consumption_projections (year, us_dc_consumption_twh, us_dc_pct_of_total, global_dc_consumption_twh, data_type, source) VALUES
(2020, 150, 3.5, 350, 'actual', 'IEA estimate'),
(2021, 160, 3.7, 370, 'actual', 'IEA estimate'),
(2022, 170, 3.9, 390, 'actual', 'IEA estimate'),
(2023, 176, 4.4, 415, 'actual', 'IEA Energy and AI Report'),
(2024, 183, 4.4, 415, 'actual', 'IEA Energy and AI Report'),
(2025, 220, 5.0, 500, 'projection', 'IEA Base Case'),
(2026, 260, 5.8, 580, 'projection', 'IEA Base Case'),
(2027, 305, 6.5, 670, 'projection', 'IEA Base Case'),
(2028, 350, 7.5, 770, 'projection', 'IEA Base Case / DOE range midpoint'),
(2029, 385, 8.2, 860, 'projection', 'IEA Base Case'),
(2030, 423, 9.0, 945, 'projection', 'IEA Base Case');

-- ================================================================
-- POPULATE VIRGINIA METRICS
-- Source: JLARC (Dec 2024), Dominion Energy Q4 2024
-- ================================================================

INSERT INTO virginia_metrics (metric, value, unit, year, source, notes) VALUES
('dc_grid_share', 26, 'percent', 2023, 'Pew Research / EIA', 'Data centers share of VA electricity'),
('dc_electricity_growth_8yr', 231, 'percent', 2024, 'Dominion Energy', '8-year growth rate'),
('contracted_capacity_jul2024', 21, 'GW', 2024, 'Dominion Q4 2024', 'July 2024 pipeline'),
('contracted_capacity_dec2024', 40, 'GW', 2024, 'Dominion Q4 2024', 'December 2024 pipeline'),
('capacity_growth_6mo', 88, 'percent', 2024, 'Dominion Q4 2024', '6-month increase'),
('connected_datacenters', 450, 'count', 2024, 'Dominion Q4 2024', 'Total connected facilities'),
('connected_capacity', 9, 'GW', 2024, 'Dominion Q4 2024', 'Total connected capacity'),
('dominion_capex_5yr', 50.1, 'billion_usd', 2025, 'Dominion Q4 2024', 'Planned capital expenditure 2025-2029'),
('dominion_va_share', 80, 'percent', 2025, 'Dominion Q4 2024', 'Share of capex in Virginia'),
('residential_cost_increase_low', 14, 'usd_per_month', 2040, 'JLARC', 'Low estimate monthly increase'),
('residential_cost_increase_high', 37, 'usd_per_month', 2040, 'JLARC', 'High estimate monthly increase'),
('demand_growth_with_dc', 183, 'percent', 2040, 'JLARC', 'Growth if unconstrained DC expansion'),
('demand_growth_without_dc', 15, 'percent', 2040, 'JLARC', 'Growth without new DC demand'),
('annual_subsidy', 732, 'million_usd', 2024, 'VA ACFR', 'State subsidies'),
('total_subsidy_with_local', 929, 'million_usd', 2023, 'JLARC', 'State + local + regional'),
('roi_per_dollar', 0.48, 'ratio', 2023, 'JLARC', 'Return per $1 of subsidy'),
('public_health_cost_low', 190, 'million_usd', 2024, 'Caltech/UCR Study', 'Regional health burden (low)'),
('public_health_cost_high', 260, 'million_usd', 2024, 'Caltech/UCR Study', 'Regional health burden (high)'),
('public_health_cost_max_permitted', 2600, 'million_usd', 2030, 'Caltech/UCR Study', 'If generators emit at max permitted');

-- ================================================================
-- POPULATE PRICE PROJECTIONS
-- Source: EIA (historical), Carnegie Mellon (projections)
-- ================================================================

INSERT INTO price_projections (year, national_avg_price_cents, national_projected_increase_pct, virginia_projected_increase_pct, data_type, source) VALUES
(2020, 11.23, 0, 0, 'actual', 'EIA'),
(2021, 11.66, 0, 0, 'actual', 'EIA'),
(2022, 12.96, 0, 0, 'actual', 'EIA'),
(2023, 13.47, 0, 0, 'actual', 'EIA'),
(2024, 13.73, 0, 0, 'actual', 'EIA'),
(2025, 14.10, 2.7, 5.0, 'projection', 'Linear interpolation to CMU 2030 target'),
(2026, 14.50, 5.6, 10.0, 'projection', 'Linear interpolation to CMU 2030 target'),
(2027, 14.90, 8.5, 15.0, 'projection', 'Linear interpolation to CMU 2030 target'),
(2028, 15.30, 11.4, 18.0, 'projection', 'Linear interpolation to CMU 2030 target'),
(2029, 15.60, 13.6, 21.0, 'projection', 'Linear interpolation to CMU 2030 target'),
(2030, 15.83, 15.3, 25.0, 'projection', 'Carnegie Mellon University study');

-- ================================================================
-- POPULATE PUBLIC HEALTH IMPACT
-- Source: Caltech/UC Riverside Study (2024)
-- ================================================================

INSERT INTO public_health_impact (metric, value_2023, value_2030_projected, unit, source, notes) VALUES
('total_public_health_cost', 6000, 20000, 'million_usd', 'Caltech/UCR Study', 'Annual public health burden'),
('premature_deaths', NULL, 1300, 'count', 'Caltech/UCR Study', 'Range: 940-1590'),
('asthma_symptom_cases', NULL, 600000, 'count', 'Caltech/UCR Study', 'Projected by 2030'),
('northern_va_regional_health_cost', NULL, 260, 'million_usd', 'Caltech/UCR Study', 'High estimate for NoVA region'),
('low_income_burden_multiplier', NULL, 200, 'times', 'Consumer Federation', 'Burden vs less-impacted households'),
('ca_dc_in_top10_polluted_areas', 33, NULL, 'percent', 'TechPolicy.Press', 'CA DCs in most polluted census tracts'),
('diesel_generator_nox_vs_natgas', 600, NULL, 'times', 'Caltech/UCR Study', 'NOx emission rate comparison');

-- ================================================================
-- VERIFICATION QUERIES
-- Run these to confirm data loaded correctly
-- ================================================================

-- Check date dimension
SELECT 
    MIN(full_date) as earliest_date,
    MAX(full_date) as latest_date,
    COUNT(*) as total_days
FROM dim_date;
-- Expected: 2020-01-01 to 2035-12-31, ~5844 days

-- Check state subsidies
SELECT COUNT(*) as states, SUM(annual_subsidy_millions) as total_disclosed
FROM state_subsidies
WHERE annual_subsidy_millions IS NOT NULL;
-- Expected: 13 states, ~3,193 million

-- Check subsidy timeline
SELECT state, MIN(year) as start_year, MAX(year) as end_year, COUNT(*) as records
FROM subsidy_timeline
GROUP BY state;
-- Expected: IL (2020-2024), TX (2023-2030), VA (2022-2024)

-- Check projections
SELECT data_type, COUNT(*) as records
FROM dc_consumption_projections
GROUP BY data_type;
-- Expected: actual (5), projection (6)

-- ================================================================
-- COMMENTS FOR DOCUMENTATION
-- ================================================================

COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis (2020-2035)';
COMMENT ON TABLE dim_datacenters IS 'Dimension table storing U.S. datacenter locations and specifications';
COMMENT ON TABLE fact_electricity_prices IS 'Historical electricity prices from EIA (your data)';
COMMENT ON TABLE state_subsidies IS 'Annual state subsidies from Good Jobs First (April 2025)';
COMMENT ON TABLE subsidy_timeline IS 'Historical subsidy growth for timeline charts';
COMMENT ON TABLE dc_consumption_projections IS 'IEA datacenter consumption projections through 2030';
COMMENT ON TABLE virginia_metrics IS 'Virginia-specific metrics from JLARC and Dominion';
COMMENT ON TABLE price_projections IS 'Price projections based on Carnegie Mellon study';
COMMENT ON TABLE public_health_impact IS 'Public health costs from Caltech/UCR study';

-- ================================================================
-- DONE! 
-- Next steps:
-- 1. Load your datacenters_clean.csv into dim_datacenters
-- 2. Load your eia_prices_clean.csv into fact_electricity_prices
-- 3. Connect Power BI to this database
-- ================================================================
