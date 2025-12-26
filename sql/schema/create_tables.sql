-- PostgreSQL Database Schema for AI Datacenter Energy Impact
-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_ai_metrics CASCADE;
DROP TABLE IF EXISTS fact_grid_impact CASCADE;
DROP TABLE IF EXISTS fact_electricity_prices CASCADE;
DROP TABLE IF EXISTS fact_energy_consumption CASCADE;
DROP TABLE IF EXISTS dim_energy_regions CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_datacenters CASCADE;

-- Datacenters Dimension
CREATE TABLE dim_datacenters (
    datacenter_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    company VARCHAR(100) NOT NULL,
    location_city VARCHAR(100),
    location_state CHAR(2) NOT NULL,
    location_zipcode VARCHAR(10),
    latitude DECIMAL(10,8) CHECK (latitude BETWEEN 24.0 AND 71.0),
    longitude DECIMAL(11,8) CHECK (longitude BETWEEN -180.0 AND -66.0),
    capacity_mw DECIMAL(10,2) CHECK (capacity_mw >= 0),
    is_ai_focused BOOLEAN DEFAULT FALSE,
    opening_date DATE,
    renewable_energy_pct DECIMAL(5,2) CHECK (renewable_energy_pct BETWEEN 0 AND 100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_us_state CHECK (
        location_state IN (
            'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
            'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
            'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
            'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
            'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
        )
    )
);

-- Date Dimension
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

-- Energy Regions
CREATE TABLE dim_energy_regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL UNIQUE,
    region_type VARCHAR(20) CHECK (region_type IN ('state', 'iso_rto')),
    grid_operator VARCHAR(100),
    states_covered TEXT[],
    total_capacity_mw DECIMAL(12,2),
    renewable_capacity_mw DECIMAL(12,2),
    description TEXT
);

-- Energy Consumption Fact
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

-- Electricity Prices Fact
CREATE TABLE fact_electricity_prices (
    price_id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    date_id INTEGER NOT NULL,
    price_per_kwh DECIMAL(10,4) CHECK (price_per_kwh >= 0),
    price_cents_per_kwh DECIMAL(10,4) CHECK (price_cents_per_kwh >= 0),
    sales_mwh DECIMAL(15,2),
    price_type VARCHAR(50) CHECK (price_type IN ('wholesale', 'retail', 'average')),
    sector VARCHAR(50),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE CASCADE
);

-- Grid Impact Fact
CREATE TABLE fact_grid_impact (
    impact_id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    date_id INTEGER NOT NULL,
    datacenter_load_mw DECIMAL(10,2) CHECK (datacenter_load_mw >= 0),
    total_grid_load_mw DECIMAL(10,2) CHECK (total_grid_load_mw >= 0),
    datacenter_percentage DECIMAL(5,2) CHECK (datacenter_percentage BETWEEN 0 AND 100),
    peak_demand_mw DECIMAL(10,2),
    reliability_index DECIMAL(5,2),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE CASCADE
);

-- AI Metrics Fact
CREATE TABLE fact_ai_metrics (
    metric_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL,
    gpu_shipments INTEGER CHECK (gpu_shipments >= 0),
    ai_market_size_usd DECIMAL(15,2),
    training_runs_count INTEGER,
    model_size_parameters BIGINT,
    source VARCHAR(100),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE CASCADE
);

-- Create Indexes
CREATE INDEX idx_energy_consumption_datacenter ON fact_energy_consumption(datacenter_id);
CREATE INDEX idx_energy_consumption_date ON fact_energy_consumption(date_id);
CREATE INDEX idx_electricity_prices_date ON fact_electricity_prices(date_id);
CREATE INDEX idx_electricity_prices_region ON fact_electricity_prices(region);
CREATE INDEX idx_grid_impact_date ON fact_grid_impact(date_id);
CREATE INDEX idx_grid_impact_region ON fact_grid_impact(region);
CREATE INDEX idx_datacenters_state ON dim_datacenters(location_state);
CREATE INDEX idx_datacenters_company ON dim_datacenters(company);
CREATE INDEX idx_datacenters_ai_focused ON dim_datacenters(is_ai_focused);
CREATE INDEX idx_date_full_date ON dim_date(full_date);
CREATE INDEX idx_date_year_month ON dim_date(year, month);

-- Populate Date Dimension (2020-2025)
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
    '2025-12-31'::date,
    '1 day'::interval
) AS date_series;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Database schema created successfully!';
    RAISE NOTICE 'Date dimension populated with % records', (SELECT COUNT(*) FROM dim_date);
END $$;