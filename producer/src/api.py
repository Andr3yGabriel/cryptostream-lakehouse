from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from dotenv import load_dotenv
import duckdb
import os

load_dotenv()  # Load environment variables from .env file

app = FastAPI()
gold_layer_path = "./storage/gold/crypto_metrics_1m"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

conn = duckdb.connect()

print("Loading Delta Lake extension for DuckDB...")
conn.execute("SET TimeZone='UTC';")
conn.execute("INSTALL delta;")
conn.execute("LOAD delta;")
print("DuckDB ready for reading!")

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is not set.")

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["cryptostream"]
alerts_collection = mongo_db["alerts"]

@app.get("/metrics/{coin_id}")
def get_metrics(coin_id: str):
    query = f"""
        SELECT 
            epoch("window".start) AS window_start, 
            epoch("window".end) AS window_end,
            coin_id,
            avg_price,
            min_price,
            max_price
        FROM delta_scan('{gold_layer_path}')
        WHERE coin_id = '{coin_id}'
        ORDER BY window_start DESC
        LIMIT 10
    """
    result_df = conn.execute(query).df()

    return result_df.to_dict(orient="records")
    
@app.get("/alerts")
def get_alerts():
    alerts_cursor = alerts_collection.find().sort("timestamp", -1).limit(20)
    
    alerts = []
    for doc in alerts_cursor:
        # Convert ObjectId to string for JSON serialization
        doc["_id"] = str(doc["_id"])
        alerts.append(doc)
    
    return alerts