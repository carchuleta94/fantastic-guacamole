import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv("FRED_API_KEY")
if api_key:
    print("SUCCESS: FRED_API_KEY loaded.")
    print(f"First 6 chars: {api_key[:6]}...")
else:
    print("ERROR: FRED_API_KEY not found.")