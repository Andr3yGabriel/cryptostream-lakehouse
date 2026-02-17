from fastapi import FastAPI
import duckdb

app = FastAPI()
gold_layer_path = "./storage/gold/crypto_metrics_1m"

conn = duckdb.connect()

print("Loading Delta Lake extension for DuckDB...")
conn.execute("INSTALL delta;")
conn.execute("LOAD delta;")
print("DuckDB ready for reading!")

@app.get("/metrics/{coin_id}")
def get_metrics(coin_id: str):
    query = f"""
        SELECT 
            "window".start AS window_start, 
            "window".end AS window_end,
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
    
    result_df["window_start"] = result_df["window_start"].astype(str)
    result_df["window_end"] = result_df["window_end"].astype(str)

    return result_df.to_dict(orient="records")
    