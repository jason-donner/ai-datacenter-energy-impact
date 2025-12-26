"""
Data Cleaning and Validation Module - Clean Version
"""
import pandas as pd
import numpy as np
import os
import sys
from glob import glob

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import RAW_DATA_DIR, PROCESSED_DATA_DIR, US_STATES
except ImportError:
    print("ERROR: Could not import config.py")
    sys.exit(1)


class DataCleaner:
    """Clean and validate collected data"""
    
    def __init__(self):
        self.raw_dir = RAW_DATA_DIR
        self.processed_dir = PROCESSED_DATA_DIR
    
    def get_latest_file(self, pattern):
        """Get the most recent file matching pattern"""
        files = glob(os.path.join(self.raw_dir, pattern))
        if not files:
            raise FileNotFoundError(f"No files found matching: {pattern}")
        return max(files, key=os.path.getctime)
    
    def clean_eia_prices(self):
        """Clean EIA electricity price data"""
        print("\n🧹 Cleaning EIA price data...")
        
        file_path = self.get_latest_file('eia_electricity_prices_*.csv')
        df = pd.read_csv(file_path)
        
        print(f"  Loaded {len(df)} records from {os.path.basename(file_path)}")
        
        df = df.rename(columns={
            'period': 'date',
            'stateid': 'state',
            'price': 'price_cents_per_kwh',
            'sales': 'sales_mwh'
        })
        
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m')
        
        initial_count = len(df)
        df = df.dropna(subset=['price_cents_per_kwh', 'sales_mwh'])
        print(f"  Removed {initial_count - len(df)} records with missing values")
        
        df = df[
            (df['price_cents_per_kwh'] >= 1) &
            (df['price_cents_per_kwh'] <= 100)
        ]
        
        df = df[df['state'].isin(US_STATES)]
        
        df['price_per_kwh'] = df['price_cents_per_kwh'] / 100
        
        output_file = os.path.join(self.processed_dir, 'eia_prices_clean.csv')
        df.to_csv(output_file, index=False)
        print(f"  ✅ Saved {len(df)} clean records to {output_file}")
        
        return df
    
    def clean_datacenter_data(self):
        """Clean datacenter data"""
        print("\n🧹 Cleaning datacenter data...")
        
        file_path = self.get_latest_file('datacenters_*.csv')
        df = pd.read_csv(file_path)
        
        print(f"  Loaded {len(df)} records from {os.path.basename(file_path)}")
        
        df['state'] = df['state'].str.upper().str.strip()
        
        df = df[
            (df['latitude'] >= 24.0) & (df['latitude'] <= 71.0) &
            (df['longitude'] >= -180.0) & (df['longitude'] <= -66.0)
        ]
        
        df['renewable_pct'] = df['renewable_pct'].clip(0, 100)
        
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        
        output_file = os.path.join(self.processed_dir, 'datacenters_clean.csv')
        df.to_csv(output_file, index=False)
        print(f"  ✅ Saved {len(df)} clean records to {output_file}")
        
        return df
    
    def generate_data_quality_report(self):
        """Generate a data quality summary report"""
        print("\n📋 Generating data quality report...")
        
        report = []
        report.append("=" * 60)
        report.append("DATA QUALITY REPORT")
        report.append("=" * 60)
        
        for filename in ['eia_prices_clean.csv', 'datacenters_clean.csv']:
            filepath = os.path.join(self.processed_dir, filename)
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                report.append(f"\n{filename}:")
                report.append(f"  Rows: {len(df)}")
                report.append(f"  Columns: {len(df.columns)}")
                report.append(f"  Missing values: {df.isnull().sum().sum()}")
                report.append(f"  Duplicate rows: {df.duplicated().sum()}")
        
        report.append("\n" + "=" * 60)
        
        report_text = "\n".join(report)
        print(report_text)
        
        report_file = os.path.join(self.processed_dir, 'data_quality_report.txt')
        with open(report_file, 'w') as f:
            f.write(report_text)
        
        return report_text
    
    def run_all_cleaning(self):
        """Run all cleaning tasks"""
        print("=" * 60)
        print("🚀 Starting Data Cleaning Process")
        print("=" * 60)
        
        eia_clean = self.clean_eia_prices()
        dc_clean = self.clean_datacenter_data()
        
        report = self.generate_data_quality_report()
        
        print("\n✅ Data cleaning complete!")
        
        return {
            'eia_prices': eia_clean,
            'datacenters': dc_clean
        }


if __name__ == "__main__":
    cleaner = DataCleaner()
    results = cleaner.run_all_cleaning()
