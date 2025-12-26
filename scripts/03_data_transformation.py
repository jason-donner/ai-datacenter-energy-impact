"""
Database Loading Module
Load cleaned data into PostgreSQL database
"""
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_CONFIG, PROCESSED_DATA_DIR
except ImportError:
    print("ERROR: Could not import config.py")
    sys.exit(1)


class DatabaseLoader:
    """Load data into PostgreSQL database"""
    
    def __init__(self):
        self.db_config = DB_CONFIG
        self.processed_dir = PROCESSED_DATA_DIR
        
        # Create SQLAlchemy engine
        from urllib.parse import quote_plus
        password = quote_plus(DB_CONFIG['password'])
        connection_string = (
            f"postgresql://{DB_CONFIG['user']}:{password}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        self.engine = create_engine(connection_string)
        
        # Create psycopg2 connection
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()
    
    def load_datacenters(self):
        """Load datacenter dimension table"""
        print("\n🏢 Loading datacenters...")
        
        # Read cleaned data
        df = pd.read_csv(os.path.join(self.processed_dir, 'datacenters_clean.csv'))
        
        # Map CSV columns to database columns
        df = df.rename(columns={
            'city': 'location_city',
            'state': 'location_state',
            'renewable_pct': 'renewable_energy_pct'
        })
        
        # Ensure proper data types
        df['is_ai_focused'] = df['is_ai_focused'].astype(bool)
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        
        # Select only columns that exist in database
        columns_to_load = [
            'name', 'company', 'location_city', 'location_state',
            'latitude', 'longitude', 'capacity_mw', 'is_ai_focused',
            'opening_date', 'renewable_energy_pct'
        ]
        
        df_to_load = df[columns_to_load]
        
        # Load to database
        df_to_load.to_sql(
            'dim_datacenters',
            self.engine,
            if_exists='append',
            index=False
        )
        
        print(f"  ✅ Loaded {len(df)} datacenter records")
        return len(df)
    
    def load_electricity_prices(self):
        """Load electricity prices fact table"""
        print("\n💰 Loading electricity prices...")
        
        df = pd.read_csv(os.path.join(self.processed_dir, 'eia_prices_clean.csv'))
        
        # Get date_ids from dim_date
        df['date'] = pd.to_datetime(df['date'])
        
        # For each row, get the corresponding date_id
        prices_with_ids = []
        
        print(f"  Processing {len(df)} price records...")
        for idx, row in df.iterrows():
            if idx % 500 == 0:
                print(f"  Progress: {idx}/{len(df)}")
            
            # Get date_id
            self.cursor.execute(
                "SELECT date_id FROM dim_date WHERE full_date = %s",
                (row['date'].date(),)
            )
            result = self.cursor.fetchone()
            
            if result:
                date_id = result[0]
                prices_with_ids.append({
                    'region': row['state'],
                    'date_id': date_id,
                    'price_per_kwh': row['price_per_kwh'],
                    'price_cents_per_kwh': row['price_cents_per_kwh'],
                    'sales_mwh': row['sales_mwh'],
                    'price_type': 'retail',
                    'sector': 'all'
                })
        
        # Convert to DataFrame and load
        df_load = pd.DataFrame(prices_with_ids)
        
        if len(df_load) > 0:
            df_load.to_sql(
                'fact_electricity_prices',
                self.engine,
                if_exists='append',
                index=False
            )
            print(f"  ✅ Loaded {len(df_load)} price records")
        else:
            print("  ⚠️ No price records to load")
        
        return len(df_load)
    
    def generate_sample_energy_consumption(self):
        """
        Generate sample energy consumption data for datacenters
        """
        print("\n⚡ Generating sample energy consumption data...")
        
        # Get all datacenters
        dc_df = pd.read_sql(
            "SELECT datacenter_id, capacity_mw, is_ai_focused, renewable_energy_pct FROM dim_datacenters",
            self.engine
        )
        
        # Get date range (monthly from 2020-01 to 2024-12)
        dates_df = pd.read_sql(
            """SELECT date_id, full_date 
               FROM dim_date 
               WHERE EXTRACT(DAY FROM full_date) = 1 
               AND full_date >= '2020-01-01' 
               AND full_date <= '2024-12-01'""",
            self.engine
        )
        
        consumption_records = []
        
        print(f"  Generating {len(dc_df)} datacenters × {len(dates_df)} months...")
        
        import numpy as np
        
        for _, dc in dc_df.iterrows():
            for _, date in dates_df.iterrows():
                # Estimate monthly consumption based on capacity
                # AI datacenters run at higher utilization
                if dc['is_ai_focused']:
                    utilization = 0.85 + (0.10 * np.random.random())  # 85-95%
                    pue = 1.15 + (0.15 * np.random.random())  # 1.15-1.30
                else:
                    utilization = 0.70 + (0.20 * np.random.random())  # 70-90%
                    pue = 1.35 + (0.25 * np.random.random())  # 1.35-1.60
                
                # Monthly consumption (MW * hours in month * utilization)
                hours_in_month = 730  # Average
                monthly_consumption = dc['capacity_mw'] * hours_in_month * utilization
                
                # Calculate renewable energy
                renewable_pct = dc['renewable_energy_pct'] if pd.notna(dc['renewable_energy_pct']) else 0
                renewable_mwh = monthly_consumption * (renewable_pct / 100)
                
                consumption_records.append({
                    'datacenter_id': int(dc['datacenter_id']),
                    'date_id': int(date['date_id']),
                    'energy_consumed_mwh': round(monthly_consumption, 2),
                    'renewable_energy_mwh': round(renewable_mwh, 2),
                    'pue_ratio': round(pue, 2),
                    'source': 'estimated'
                })
        
        # Load to database
        df_consumption = pd.DataFrame(consumption_records)
        df_consumption.to_sql(
            'fact_energy_consumption',
            self.engine,
            if_exists='append',
            index=False
        )
        
        print(f"  ✅ Generated {len(consumption_records)} consumption records")
        return len(consumption_records)
    
    def verify_data_integrity(self):
        """Verify all data was loaded correctly"""
        print("\n🔍 Verifying data integrity...")
        
        tables = [
            'dim_datacenters',
            'dim_date',
            'fact_electricity_prices',
            'fact_energy_consumption'
        ]
        
        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            print(f"  {table}: {count} records")
        
        # Check for orphaned records
        print("\n  Checking referential integrity...")
        
        self.cursor.execute("""
            SELECT COUNT(*) 
            FROM fact_energy_consumption ec
            LEFT JOIN dim_datacenters dc ON ec.datacenter_id = dc.datacenter_id
            WHERE dc.datacenter_id IS NULL
        """)
        orphans = self.cursor.fetchone()[0]
        
        if orphans == 0:
            print("  ✅ No orphaned energy consumption records")
        else:
            print(f"  ⚠️ Found {orphans} orphaned energy consumption records")
        
        return True
    
    def run_full_load(self):
        """Execute complete data loading process"""
        print("=" * 60)
        print("🚀 Starting Database Loading Process")
        print("=" * 60)
        
        try:
            # Load dimension tables first
            dc_count = self.load_datacenters()
            
            # Load fact tables
            price_count = self.load_electricity_prices()
            consumption_count = self.generate_sample_energy_consumption()
            
            # Verify
            self.verify_data_integrity()
            
            # Commit transaction
            self.conn.commit()
            
            print("\n" + "=" * 60)
            print("✅ Database Loading Complete!")
            print("=" * 60)
            print("\nSummary:")
            print(f"  - Datacenters: {dc_count}")
            print(f"  - Electricity prices: {price_count}")
            print(f"  - Energy consumption: {consumption_count}")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error during loading: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
            return False
        
        finally:
            self.cursor.close()
            self.conn.close()


if __name__ == "__main__":
    loader = DatabaseLoader()
    success = loader.run_full_load()
    
    if success:
        print("\n✅ Ready for Power BI connection!")
    else:
        print("\n❌ Loading failed. Check errors above.")
