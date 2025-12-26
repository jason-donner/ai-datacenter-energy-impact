"""
Data Collection Module
Collects data from EIA API and creates sample datacenter data
"""
import requests
import pandas as pd
import json
import time
from datetime import datetime
import os
import sys

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import (
        EIA_API_KEY, DATA_START_DATE, DATA_END_DATE,
        US_STATES, RAW_DATA_DIR
    )
except ImportError:
    print("ERROR: Could not import config.py")
    print("Make sure config.py exists in the scripts folder")
    sys.exit(1)


class DataCollector:
    """Main data collection class"""
    
    def __init__(self):
        self.eia_api_key = EIA_API_KEY
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if not self.eia_api_key:
            print("ERROR: EIA_API_KEY not found in .env file")
            sys.exit(1)
    
    def collect_eia_electricity_prices(self):
        """
        Collect state-level retail electricity prices from EIA
        """
        print("\n📊 Collecting EIA electricity price data...")
        
        all_data = []
        
        for state in US_STATES:
            print(f"  Fetching data for {state}...", end=' ')
            
            url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
            params = {
                'api_key': self.eia_api_key,
                'frequency': 'monthly',
                'data[0]': 'price',
                'data[1]': 'sales',
                'facets[stateid][]': state,
                'facets[sectorid][]': 'ALL',
                'start': DATA_START_DATE,
                'end': DATA_END_DATE,
                'sort[0][column]': 'period',
                'sort[0][direction]': 'asc',
                'offset': 0,
                'length': 5000
            }
            
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if 'response' in data and 'data' in data['response']:
                    state_data = data['response']['data']
                    all_data.extend(state_data)
                    print(f"✅ {len(state_data)} records")
                else:
                    print("⚠️ No data")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error: {e}")
                continue
        
        df = pd.DataFrame(all_data)
        
        output_file = os.path.join(RAW_DATA_DIR, f'eia_electricity_prices_{self.timestamp}.csv')
        df.to_csv(output_file, index=False)
        print(f"\n✅ Saved {len(df)} records to {output_file}")
        
        return df
    
    def collect_eia_generation_data(self):
        """
        Collect electricity generation data from EIA
        """
        print("\n⚡ Collecting EIA generation data...")
        
        url = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
        params = {
            'api_key': self.eia_api_key,
            'frequency': 'monthly',
            'data[0]': 'generation',
            'start': DATA_START_DATE,
            'end': DATA_END_DATE,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'offset': 0,
            'length': 5000
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'response' in data and 'data' in data['response']:
                df = pd.DataFrame(data['response']['data'])
                
                output_file = os.path.join(RAW_DATA_DIR, f'eia_generation_{self.timestamp}.csv')
                df.to_csv(output_file, index=False)
                print(f"✅ Saved {len(df)} records to {output_file}")
                
                return df
            else:
                print("⚠️ No data returned from API")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error collecting generation data: {e}")
            return pd.DataFrame()
    
    def create_sample_datacenter_data(self):
        """
        Create sample datacenter data
        """
        print("\n🏢 Creating sample datacenter dataset...")
        
        datacenters = [
            {
                'name': 'Virginia Data Center 1',
                'company': 'AWS',
                'city': 'Ashburn',
                'state': 'VA',
                'latitude': 39.0438,
                'longitude': -77.4874,
                'capacity_mw': 150,
                'is_ai_focused': True,
                'opening_date': '2020-06-15',
                'renewable_pct': 65
            },
            {
                'name': 'Silicon Valley DC',
                'company': 'Google',
                'city': 'Mountain View',
                'state': 'CA',
                'latitude': 37.4220,
                'longitude': -122.0841,
                'capacity_mw': 120,
                'is_ai_focused': True,
                'opening_date': '2019-03-20',
                'renewable_pct': 85
            },
            {
                'name': 'Texas Mega Center',
                'company': 'Microsoft',
                'city': 'San Antonio',
                'state': 'TX',
                'latitude': 29.4241,
                'longitude': -98.4936,
                'capacity_mw': 200,
                'is_ai_focused': True,
                'opening_date': '2021-01-10',
                'renewable_pct': 45
            },
            {
                'name': 'Oregon Data Hub',
                'company': 'Meta',
                'city': 'Prineville',
                'state': 'OR',
                'latitude': 44.2999,
                'longitude': -120.8342,
                'capacity_mw': 100,
                'is_ai_focused': False,
                'opening_date': '2018-09-01',
                'renewable_pct': 90
            },
            {
                'name': 'Iowa Compute Center',
                'company': 'Google',
                'city': 'Council Bluffs',
                'state': 'IA',
                'latitude': 41.2619,
                'longitude': -95.8608,
                'capacity_mw': 130,
                'is_ai_focused': True,
                'opening_date': '2020-11-05',
                'renewable_pct': 95
            },
            {
                'name': 'Georgia AI Facility',
                'company': 'AWS',
                'city': 'Atlanta',
                'state': 'GA',
                'latitude': 33.7490,
                'longitude': -84.3880,
                'capacity_mw': 110,
                'is_ai_focused': True,
                'opening_date': '2022-04-15',
                'renewable_pct': 55
            },
            {
                'name': 'Illinois Data Complex',
                'company': 'Microsoft',
                'city': 'Chicago',
                'state': 'IL',
                'latitude': 41.8781,
                'longitude': -87.6298,
                'capacity_mw': 95,
                'is_ai_focused': False,
                'opening_date': '2019-07-22',
                'renewable_pct': 60
            },
            {
                'name': 'North Carolina Center',
                'company': 'Apple',
                'city': 'Maiden',
                'state': 'NC',
                'latitude': 35.5732,
                'longitude': -81.2212,
                'capacity_mw': 140,
                'is_ai_focused': False,
                'opening_date': '2018-12-01',
                'renewable_pct': 100
            },
        ]
        
        df = pd.DataFrame(datacenters)
        
        output_file = os.path.join(RAW_DATA_DIR, f'datacenters_sample_{self.timestamp}.csv')
        df.to_csv(output_file, index=False)
        print(f"✅ Created {len(df)} sample datacenter records")
        
        return df
    
    def run_all_collections(self):
        """Run all data collection tasks"""
        print("=" * 60)
        print("🚀 Starting Data Collection Process")
        print("=" * 60)
        
        eia_prices = self.collect_eia_electricity_prices()
        eia_generation = self.collect_eia_generation_data()
        datacenters = self.create_sample_datacenter_data()
        
        print("\n" + "=" * 60)
        print("✅ Data Collection Complete!")
        print("=" * 60)
        print(f"\nData saved to: {RAW_DATA_DIR}")
        print("\nSummary:")
        print(f"  - EIA Prices: {len(eia_prices)} records")
        print(f"  - EIA Generation: {len(eia_generation)} records")
        print(f"  - Datacenters: {len(datacenters)} records")
        print("\nNote: FRED data collection skipped (optional source)")
        
        return {
            'eia_prices': eia_prices,
            'eia_generation': eia_generation,
            'datacenters': datacenters
        }


if __name__ == "__main__":
    collector = DataCollector()
    results = collector.run_all_collections()