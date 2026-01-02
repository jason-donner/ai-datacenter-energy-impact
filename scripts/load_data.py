"""
Data Loading Script for AI Datacenter Impact Dashboard
========================================================
This script loads your CSV files into the PostgreSQL database.

Prerequisites:
- PostgreSQL database created
- Schema script (complete_database_schema.sql) already run
- Python packages: pandas, psycopg2, sqlalchemy, python-dotenv

Usage:
    cd C:\\Users\\Jason\\Documents\\ai-datacenter-energy-impact
    python scripts\\load_data.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv

# ================================================================
# LOAD ENVIRONMENT VARIABLES
# ================================================================

# Your project paths
PROJECT_DIR = r"C:\Users\Jason\Documents\ai-datacenter-energy-impact"
ENV_PATH = os.path.join(PROJECT_DIR, ".env")

# Load environment variables
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
    print(f"✅ Loaded .env from: {ENV_PATH}")
else:
    print(f"❌ .env file not found at: {ENV_PATH}")
    print("   Please create this file with your database credentials.")
    exit(1)

# ================================================================
# DATABASE CONFIGURATION (from .env)
# ================================================================

DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'datacenter_energy'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Verify credentials loaded
if not DATABASE_CONFIG['user'] or not DATABASE_CONFIG['password']:
    print("❌ Database credentials not found in .env file!")
    print("   Make sure your .env file contains:")
    print("   DB_HOST=localhost")
    print("   DB_PORT=5432")
    print("   DB_NAME=datacenter_energy")
    print("   DB_USER=your_username")
    print("   DB_PASSWORD=your_password")
    exit(1)

# ================================================================
# FILE PATHS - Your data files in data/processed/
# ================================================================

DATA_DIR = os.path.join(PROJECT_DIR, "data", "processed")

DATA_FILES = {
    'datacenters': os.path.join(DATA_DIR, 'datacenters_clean.csv'),
    'eia_prices': os.path.join(DATA_DIR, 'eia_prices_clean.csv'),
    # These are already loaded by the SQL schema, but listed for reference:
    'dc_projections': os.path.join(DATA_DIR, 'dc_consumption_projections.csv'),
    'price_projections': os.path.join(DATA_DIR, 'price_projections.csv'),
    'public_health': os.path.join(DATA_DIR, 'public_health_impact.csv'),
    'state_subsidies': os.path.join(DATA_DIR, 'state_subsidies.csv'),
    'subsidy_timeline': os.path.join(DATA_DIR, 'subsidy_timeline.csv'),
    'virginia_metrics': os.path.join(DATA_DIR, 'virginia_metrics.csv'),
}

# ================================================================
# DATABASE CONNECTION
# ================================================================

def get_engine():
    """Create SQLAlchemy engine for PostgreSQL connection."""
    connection_string = (
        f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}"
        f"@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"
    )
    return create_engine(connection_string)

# ================================================================
# DATA LOADING FUNCTIONS
# ================================================================

def load_datacenters(engine, filepath):
    """Load datacenters CSV into dim_datacenters table."""
    print(f"\n📍 Loading datacenters from {filepath}...")
    
    df = pd.read_csv(filepath)
    
    # Rename columns to match database schema
    column_mapping = {
        'name': 'name',
        'company': 'company',
        'city': 'location_city',
        'state': 'location_state',
        'latitude': 'latitude',
        'longitude': 'longitude',
        'capacity_mw': 'capacity_mw',
        'opening_date': 'opening_date',
        'is_ai_focused': 'is_ai_focused',
        'renewable_pct': 'renewable_energy_pct'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Convert opening_date to proper format
    df['opening_date'] = pd.to_datetime(df['opening_date'])
    
    # Convert boolean
    df['is_ai_focused'] = df['is_ai_focused'].map({True: True, False: False, 'True': True, 'False': False})
    
    # Add timestamp
    df['last_updated'] = datetime.now()
    
    # Load to database
    df.to_sql('dim_datacenters', engine, if_exists='append', index=False)
    
    print(f"   ✅ Loaded {len(df)} datacenters")
    return len(df)

def load_eia_prices(engine, filepath):
    """Load EIA electricity prices into fact_electricity_prices table."""
    print(f"\n⚡ Loading electricity prices from {filepath}...")
    
    df = pd.read_csv(filepath)
    
    # Parse date and get date_id from dim_date
    df['date'] = pd.to_datetime(df['date'])
    
    # We need to join with dim_date to get date_id
    # First, let's get the date mapping
    with engine.connect() as conn:
        date_df = pd.read_sql("SELECT date_id, full_date FROM dim_date", conn)
    
    date_df['full_date'] = pd.to_datetime(date_df['full_date'])
    
    # Merge to get date_id
    df = df.merge(date_df, left_on='date', right_on='full_date', how='left')
    
    # Check for missing dates
    missing = df[df['date_id'].isna()]
    if len(missing) > 0:
        print(f"   ⚠️  Warning: {len(missing)} records have dates not in dim_date")
        print(f"      Date range in data: {df['date'].min()} to {df['date'].max()}")
        df = df.dropna(subset=['date_id'])
    
    # Prepare final dataframe
    price_df = pd.DataFrame({
        'region': df['state'],
        'date_id': df['date_id'].astype(int),
        'price_per_kwh': df['price_per_kwh'],
        'price_cents_per_kwh': df['price_cents_per_kwh'],
        'sales_mwh': df['sales_mwh'],
        'price_type': 'average',
        'sector': df['sectorName']
    })
    
    # Load to database
    price_df.to_sql('fact_electricity_prices', engine, if_exists='append', index=False)
    
    print(f"   ✅ Loaded {len(price_df)} price records")
    return len(price_df)

def verify_data(engine):
    """Run verification queries to confirm data loaded correctly."""
    print("\n🔍 Verifying data load...")
    
    with engine.connect() as conn:
        # Check dim_date
        result = conn.execute(text("""
            SELECT MIN(full_date) as min_date, MAX(full_date) as max_date, COUNT(*) as count 
            FROM dim_date
        """))
        row = result.fetchone()
        print(f"   dim_date: {row[2]} days ({row[0]} to {row[1]})")
        
        # Check datacenters
        result = conn.execute(text("""
            SELECT COUNT(*) as count, COUNT(DISTINCT company) as companies, 
                   COUNT(DISTINCT location_state) as states
            FROM dim_datacenters
        """))
        row = result.fetchone()
        print(f"   dim_datacenters: {row[0]} facilities, {row[1]} companies, {row[2]} states")
        
        # Check prices
        result = conn.execute(text("""
            SELECT COUNT(*) as count, COUNT(DISTINCT region) as states,
                   MIN(price_cents_per_kwh) as min_price, MAX(price_cents_per_kwh) as max_price
            FROM fact_electricity_prices
        """))
        row = result.fetchone()
        print(f"   fact_electricity_prices: {row[0]} records, {row[1]} states")
        print(f"      Price range: {row[2]:.2f}¢ to {row[3]:.2f}¢ per kWh")
        
        # Check state subsidies
        result = conn.execute(text("""
            SELECT COUNT(*) as count, SUM(annual_subsidy_millions) as total
            FROM state_subsidies
            WHERE annual_subsidy_millions IS NOT NULL
        """))
        row = result.fetchone()
        print(f"   state_subsidies: {row[0]} states with disclosed subsidies (${row[1]:,.0f}M total)")
        
        # Check subsidy timeline
        result = conn.execute(text("""
            SELECT state, COUNT(*) as records 
            FROM subsidy_timeline 
            GROUP BY state
        """))
        rows = result.fetchall()
        timeline_summary = ", ".join([f"{r[0]}: {r[1]}" for r in rows])
        print(f"   subsidy_timeline: {timeline_summary}")
        
        # Check projections
        result = conn.execute(text("""
            SELECT data_type, COUNT(*) as count 
            FROM dc_consumption_projections 
            GROUP BY data_type
        """))
        rows = result.fetchall()
        proj_summary = ", ".join([f"{r[0]}: {r[1]}" for r in rows])
        print(f"   dc_consumption_projections: {proj_summary}")

# ================================================================
# MAIN EXECUTION
# ================================================================

def main():
    print("=" * 60)
    print("AI DATACENTER IMPACT DASHBOARD - DATA LOADER")
    print("=" * 60)
    
    # Show configuration
    print(f"\n📁 Project directory: {PROJECT_DIR}")
    print(f"📂 Data directory: {DATA_DIR}")
    print(f"🗄️  Database: {DATABASE_CONFIG['database']} @ {DATABASE_CONFIG['host']}")
    
    # Verify data files exist
    print("\n📋 Checking data files...")
    for name, path in DATA_FILES.items():
        if os.path.exists(path):
            print(f"   ✅ {name}: Found")
        else:
            print(f"   ❌ {name}: NOT FOUND at {path}")
    
    # Create database connection
    print("\n🔌 Connecting to database...")
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("   ✅ Connected successfully")
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        print("\n   Check your .env file and PostgreSQL server.")
        return
    
    # Load datacenters
    if os.path.exists(DATA_FILES['datacenters']):
        load_datacenters(engine, DATA_FILES['datacenters'])
    else:
        print(f"\n⚠️  Datacenter file not found: {DATA_FILES['datacenters']}")
    
    # Load EIA prices
    if os.path.exists(DATA_FILES['eia_prices']):
        load_eia_prices(engine, DATA_FILES['eia_prices'])
    else:
        print(f"\n⚠️  EIA prices file not found: {DATA_FILES['eia_prices']}")
    
    # Verify all data
    verify_data(engine)
    
    print("\n" + "=" * 60)
    print("DATA LOAD COMPLETE!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Open Power BI Desktop")
    print("2. Get Data → PostgreSQL")
    print("3. Enter your database credentials")
    print("4. Select all tables")
    print("5. Start building your dashboard!")

if __name__ == "__main__":
    main()
