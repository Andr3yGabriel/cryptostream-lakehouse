import duckdb
import requests
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timezone
import os

load_dotenv()  # Load environment variables from .env file

WEBHOOK_URL = os.getenv("WEBHOOK_URL")
MONGO_URI = os.getenv("MONGO_URI")

conn = duckdb.connect()
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["cryptostream"]
alerts_collection = mongo_db["alerts"]
gold_layer_path = "./storage/gold/crypto_metrics_1m"

print("Loading Delta Lake extension for DuckDB...")
conn.execute("SET TimeZone='UTC';")
conn.execute("INSTALL delta;")
conn.execute("LOAD delta;")
print("DuckDB ready for reading!")

def get_metrics(coin_id: str) -> dict | None:
    """
    Fetches the most recent metrics for a given coin_id from the gold layer.
    Returns a dictionary with the metrics or None if no data is found.

    Args:
        coin_id (str): The identifier of the cryptocurrency (e.g., 'bitcoin', 'ethereum').
    Returns:
        dict | None: A dictionary containing the metrics or None if no data is found.
    """
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
        LIMIT 1
    """

    result_df = conn.execute(query).df()
    return result_df.to_dict(orient="records")[0] if not result_df.empty else None

def calculate_volatility(avg_price: float, min_price: float, max_price: float) -> float:
    """
    Calculates the volatility of a cryptocurrency based on its average, minimum, and maximum prices.

    Args:
        avg_price (float): The average price of the cryptocurrency.
        min_price (float): The minimum price of the cryptocurrency.
        max_price (float): The maximum price of the cryptocurrency.

    Returns:
        float: The calculated volatility as a percentage.
    """
    if avg_price == 0:
        return 0.0
    volatility = ((max_price - min_price) / avg_price) * 100
    return round(volatility, 2)

def generate_json_message_for_discord(metrics: dict, volatility: float) -> dict:
    """
    Generates a JSON message formatted for Discord webhook based on the provided metrics.
    """
    coin_id = metrics['coin_id'].capitalize()
    avg_price = metrics['avg_price']
    min_price = metrics['min_price']
    max_price = metrics['max_price']
    
    isSmallCoin = coin_id.lower() in ['dogecoin', 'cardano', 'ripple', 'tron', 'polkadot', 'avalanche-2', 'chainlink']
    decimal_places = 4 if isSmallCoin else 2
    # We use the already calculated volatility passed as an argument.
    emoji = "📈" if volatility > 0 else "📉"
    message = {
        "content": f"🚨 **{coin_id} Volatility Alert!** {emoji}\n"
                   f"**Average Price:** ${avg_price:.{decimal_places}f}\n"
                   f"**Min Price:** ${min_price:.{decimal_places}f}\n"
                   f"**Max Price:** ${max_price:.{decimal_places}f}\n"
                   f"**Change:** {volatility:.2f}% in the last minute!"
    }
    return message

def send_alert_if_volatile(coin_id: str, threshold: float = 1.0):
    metrics = get_metrics(coin_id)
    
    if not metrics:
        print(f"⚠️ No data found for {coin_id}. Is the Spark pipeline running?")
        return

    current_time = datetime.now(timezone.utc).timestamp()
    data_time = metrics['window_end']
    
    lag = current_time - data_time
    if lag > 300:
        print(f"⚠️ Stale data for {coin_id}. Lag: {lag:.0f}s. Skipping alert.")
        return

    # Calculate once
    volatility = calculate_volatility(metrics['avg_price'], metrics['min_price'], metrics['max_price'])
    
    print(f"🔍 Checking {coin_id.ljust(10)} | Volatility: {volatility:.4f}% | Threshold: {threshold}%")

    # We use absolute value so it triggers on both pumps (+) and dumps (-)
    if abs(volatility) > threshold:
        # Pass the calculated volatility to the generator
        message = generate_json_message_for_discord(metrics, volatility)
        response = requests.post(WEBHOOK_URL, json=message)
        
        if response.status_code != 204:
            print(f"❌ Failed to send Discord alert for {coin_id}: {response.status_code}")
        else:
            print(f"✅ ALERT SENT for {coin_id.capitalize()}!")
            
            # Save alert to MongoDB
            alert_record = {
                "coin_id": coin_id,
                "avg_price": metrics['avg_price'],
                "min_price": metrics['min_price'],
                "max_price": metrics['max_price'],
                "volatility": volatility,
                "timestamp": datetime.now(timezone.utc)  # Store in UTC
            }
            try:
                alerts_collection.insert_one(alert_record)
                print(f"💾 Alert saved to MongoDB.")
            except Exception as e:
                print(f"❌ MongoDB Error: {e}")

# Loop this script to check for volatility in any crypto every minute
if __name__ == "__main__":
    import time
    
    TEST_THRESHOLD = 0.02 
    COINS = "bitcoin,ethereum,solana,cardano,ripple,polkadot,dogecoin,chainlink,tron,avalanche-2"
    
    print(f"🚀 Alert Worker started! Monitoring {len(COINS.split(','))} coins...")
    print(f"🎯 Threshold set to: {TEST_THRESHOLD}%")
    print("-" * 50)

    while True:
        for coin in COINS.split(','):
            send_alert_if_volatile(coin, threshold=TEST_THRESHOLD)
        
        print("-" * 50)
        print("💤 Sleeping for 60 seconds...")
        time.sleep(60)