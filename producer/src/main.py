import time
import json
import requests
from kafka import KafkaProducer

# Configurações
TOPIC = "crypto-raw"
BOOTSTRAP_SERVERS = "localhost:19092"
COINS = "bitcoin,ethereum,solana,cardano,ripple,polkadot,dogecoin,chainlink,tron,avalanche-2"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

def fetch_crypto_data():
    url = f"https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": COINS,
        "order": "market_cap_desc",
        "sparkline": False
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Erro ao buscar dados: {e}")
        return None

def main():
    print(f"🚀 Iniciando Producer no tópico: {TOPIC}")
    while True:
        data = fetch_crypto_data()
        if data:
            for coin in data:
                payload = {
                    "timestamp": time.time(),
                    "coin_id": coin['id'],
                    "price": coin['current_price'],
                    "volume_24h": coin['total_volume']
                }
                producer.send(TOPIC, value=payload)
            print(f"✅ {len(data)} registros enviados para o Kafka.")
        
        # Intervalo seguro para não tomar block da API (Rate Limit)
        time.sleep(30)

if __name__ == "__main__":
    main()