"""
Configuration settings for data collection
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
EIA_API_KEY = os.getenv('EIA_API_KEY')
FRED_API_KEY = os.getenv('FRED_API_KEY')

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'datacenter_energy'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# Data Collection Settings
DATA_START_DATE = '2020-01'  # Start from January 2020
DATA_END_DATE = '2024-12'    # Through December 2024

# U.S. States (for iteration)
US_STATES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
]

# Major U.S. RTOs/ISOs
US_RTOS = {
    'CAISO': 'California ISO',
    'PJM': 'PJM Interconnection',
    'ERCOT': 'Electric Reliability Council of Texas',
    'MISO': 'Midcontinent ISO',
    'ISONE': 'ISO New England',
    'NYISO': 'New York ISO',
    'SPP': 'Southwest Power Pool'
}

# Data directories
RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed')

# Create directories if they don't exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)