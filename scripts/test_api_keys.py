import requests
from dotenv import load_dotenv
import os

load_dotenv()

# Test EIA API
print("Testing EIA API...")
eia_key = os.getenv('EIA_API_KEY')
eia_url = f"https://api.eia.gov/v2/electricity/retail-sales/data/?api_key={eia_key}&frequency=monthly&data[0]=sales&facets[stateid][]=CA&start=2024-01&end=2024-01"

try:
    response = requests.get(eia_url)
    if response.status_code == 200:
        print("✅ EIA API key is valid!")
        data = response.json()
        print(f"   Sample data points: {len(data.get('response', {}).get('data', []))}")
    else:
        print(f"❌ EIA API error: {response.status_code}")
except Exception as e:
    print(f"❌ EIA API error: {e}")

# Test FRED API
print("\nTesting FRED API...")
fred_key = os.getenv('FRED_API_KEY')
fred_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=GNPCA&api_key={fred_key}&file_type=json"

try:
    response = requests.get(fred_url)
    if response.status_code == 200:
        print("✅ FRED API key is valid!")
        data = response.json()
        print(f"   Series: {data.get('seriess', [{}])[0].get('title', 'N/A')}")
    else:
        print(f"❌ FRED API error: {response.status_code}")
except Exception as e:
    print(f"❌ FRED API error: {e}")