-- View: Datacenter Energy Consumption by Region
CREATE VIEW vw_datacenter_energy_by_region AS
SELECT 
    r.region_name,
    d.year,
    d.quarter,
    COUNT(DISTINCT dc.datacenter_id) as datacenter_count,
    SUM(ec.energy_consumed_mwh) as total_energy_mwh,
    AVG(ec.pue_ratio) as avg_pue,
    SUM(ec.energy_consumed_mwh * ec.renewable_energy_pct / 100) as renewable_energy_mwh
FROM fact_energy_consumption ec
JOIN dim_datacenters dc ON ec.datacenter_id = dc.datacenter_id
JOIN dim_date d ON ec.date_id = d.date_id
JOIN dim_energy_regions r ON dc.location_state = r.region_name
GROUP BY r.region_name, d.year, d.quarter;

-- View: Energy Price Correlation
CREATE VIEW vw_price_impact_analysis AS
SELECT 
    ep.region,
    d.year,
    d.month,
    AVG(ep.price_per_kwh) as avg_price,
    SUM(gi.datacenter_load_mw) as datacenter_load,
    SUM(gi.total_grid_load_mw) as total_load,
    AVG(gi.datacenter_percentage) as datacenter_pct
FROM fact_electricity_prices ep
JOIN dim_date d ON ep.date_id = d.date_id
JOIN fact_grid_impact gi ON ep.region = gi.region AND ep.date_id = gi.date_id
GROUP BY ep.region, d.year, d.month;

-- View: AI Growth vs Energy Consumption
CREATE VIEW vw_ai_energy_correlation AS
SELECT 
    d.year,
    d.quarter,
    SUM(ai.gpu_shipments) as total_gpu_shipments,
    AVG(ai.ai_market_size_usd) as avg_market_size,
    SUM(ec.energy_consumed_mwh) as total_energy_consumption,
    COUNT(DISTINCT CASE WHEN dc.is_ai_focused = 1 THEN dc.datacenter_id END) as ai_datacenter_count
FROM fact_ai_metrics ai
JOIN dim_date d ON ai.date_id = d.date_id
LEFT JOIN fact_energy_consumption ec ON d.date_id = ec.date_id
LEFT JOIN dim_datacenters dc ON ec.datacenter_id = dc.datacenter_id
GROUP BY d.year, d.quarter;