# CryptoStream Lakehouse

## Project Overview

CryptoStream Lakehouse is an end-to-end data pipeline that ingests near-real-time cryptocurrency market data, processes it using Spark Structured Streaming, and stores it in a Delta Lake architecture with Bronze, Silver, and Gold layers.

## Current Implementation

### Architecture

```mermaid
graph LR
    A[CoinGecko API] -->|Python Producer| B(Redpanda / Kafka)
    B -->|Spark Streaming| C[(Bronze Layer<br>Raw JSON)]
    C -->|Spark Schema-on-Read| D[(Silver Layer<br>Clean & Typed)]
    D -->|Spark Watermark| E[(Gold Layer<br>1m Aggregations)]
    E -->|DuckDB SQL| F[FastAPI]
    E -->|DuckDB SQL| H[Alert Worker]
    H -->|Threshold > X%| I(Discord Webhook)
    H -->|JSON Log| J[(MongoDB<br>NoSQL)]
    F -->|JSON Endpoint| G{Frontend Dashboard}
    J -.->|Query| F
    
    style C fill:#cd7f32,stroke:#333,stroke-width:2px,color:#fff
    style D fill:#c0c0c0,stroke:#333,stroke-width:2px,color:#000
    style E fill:#ffd700,stroke:#333,stroke-width:2px,color:#000
    style H fill:#ff5555,stroke:#333,stroke-width:2px,color:#fff
    style I fill:#5865F2,stroke:#333,stroke-width:2px,color:#fff
    style J fill:#4db33d,stroke:#333,stroke-width:2px,color:#fff
```

- **Data Source:** CoinGecko API (free tier)
- **Message Broker:** RedPanda (Kafka-compatible)
- **Processing Engine:** Apache Spark 3.5.0 with Delta Lake
- **Storage Format:** Delta Lake (Parquet-based)
- **Monitoring:** RedPanda Console

### Data Pipeline

#### 1. Producer (`producer/src/`)

Python-based producer that:

- Fetches data from CoinGecko API for 10 cryptocurrencies
- Publishes to `crypto-raw` Kafka topic every 10-15 seconds
- Includes: price, 24h volume, market cap, price changes

**Tracked Cryptocurrencies:**
Bitcoin, Ethereum, Solana, Cardano, Ripple, Polkadot, Dogecoin, Chainlink, Tron, Avalanche

#### 2. Bronze Layer (`storage/bronze/crypto_raw`)

Raw ingestion from Kafka:

- Spark Structured Streaming reads from RedPanda
- Stores raw JSON events with ingestion timestamp
- No transformations applied
- Script: `producer/src/bronze_ingestion.py`

#### 3. Silver Layer (`storage/silver/crypto_transformed`)

Cleaned and enriched data:

- Parses JSON and extracts structured fields
- Adds timestamp columns (year, month, day, hour)
- Deduplication and schema enforcement
- Script: `producer/src/silver_transformation.py`

#### 4. Gold Layer (`storage/gold/crypto_metrics_1m`)

Aggregated analytics:

- 1-minute windowed metrics (average, min, max price)
- Calculated price changes and volatility
- Ready for analytics and visualization
- Script: `producer/src/gold_aggregation.py`

#### 5. Serving Layer (`producer/src/api.py`)

High-performance API for data consumption:

- FastAPI server integrated with DuckDB
- Queries Delta Lake files directly bypassing Spark overhead
- Resolves timezone shifts and returns JSON metrics with millisecond latency

#### 6. Visualization (`dashboard/`)

Real-time Frontend Dashboard:

- Next.js application built with App Router and Tailwind CSS v4
- Interactive Candlestick charts powered by TradingView's Lightweight Charts
- Dynamic polling every 10 seconds for real-time updates

#### 7. Monitoring & Alerting (`producer/src/alert_worker.py`)

Background worker for proactive anomaly detection:

- Polls Gold Layer (DuckDB) every 60 seconds
- Calculates volatility percentage: `((Max - Min) / Min) * 100`
- Triggers Discord Webhook if volatility > Threshold (default 0.02%)
- Persists alert logs to MongoDB for historical auditing

## Project Structure

```bash
cryptostream-lakehouse/
├── dashboard/                # Next.js frontend application
│   ├── src/app/              # UI components, layout, and global styles
│   └── package.json          # Node.js dependencies
├── infra/
│   └── docker-compose.yml    # RedPanda + Spark cluster
├── producer/
│   ├── requirements.txt      # Python dependencies
│   └── src/
│       ├── main.py           # Producer application
│       ├── api.py            # FastAPI serving layer
│       ├── bronze_ingestion.py
│       ├── silver_transformation.py
│       ├── gold_aggregation.py
│       ├── verify_silver.py
│       └── verify_gold.py
├── storage/
│   ├── bronze/               # Raw data layer
│   ├── silver/               # Transformed data layer
│   ├── gold/                 # Aggregated metrics layer
│   └── checkpoints/          # Spark streaming checkpoints
└── README.MD
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- PySpark 3.5.8

### Setup

1. **Install Python Dependencies:**

   ```bash
   cd producer
   pip install -r requirements.txt
   ```

2. **Environment Configuration:**
   Create a `.env` (also check `.env.example` to build it) file in the root directory:

   ```env
   MONGO_URI=mongodb://admin:password@localhost:27017/
   MONGO_USERNAME=admin
   MONGO_PASSWORD=password
   WEBHOOK_URL=[https://discord.com/api/webhooks/YOUR_WEBHOOK_URL](https://discord.com/api/webhooks/YOUR_WEBHOOK_URL)
   ```

3. **Start Infrastructure:**

   ```bash
   # Run from project root to load .env correctly
   docker-compose --env-file .env -f infra/docker-compose.yml up -d
   ```

   - RedPanda: [http://localhost:19092](http://localhost:19092)
   - Mongo Express: [http://localhost:8081](http://localhost:8081)

4. **Run Producer:**

   ```bash
   cd producer/src
   python main.py
   ```

5. **Run Streaming Pipeline:**

   ```bash
   # Bronze ingestion
   python bronze_ingestion.py
   
   # Silver transformation (in separate terminal)
   python silver_transformation.py
   
   # Gold aggregation (in separate terminal)
   python gold_aggregation.py
   ```

6. **Start the API Server:**

   ```bash
   # In a separate terminal, from the producer/src directory
   uvicorn api:app --reload
   ```

7. **Run the Frontedn Dashboard:**

   ```bash
   # In a separate terminal, from the project root
   cd dashboard
   yarn install
   yarn dev
   ```

Access the dashboard at [http://localhost:3000](http://localhost:3000)

## Features Implemented

✅ Continuous data ingestion from CoinGecko API  
✅ Kafka/RedPanda message streaming  
✅ Spark Structured Streaming processing  
✅ Delta Lake storage with ACID transactions  
✅ Bronze → Silver → Gold medallion architecture  
✅ Windowed aggregations (1-minute windows)  
✅ Checkpointing for fault tolerance  
✅ Local development environment via Docker  
✅ RESTful API serving Gold metrics with millisecond latency  
✅ In-memory analytical queries using DuckDB  
✅ Interactive Next.js Frontend with Dark Mode & Glassmorphism  
✅ Real-time Candlestick charting with TradingView integration  

## Technology Stack

**Backend & Data Engineering:**

- **Python 3.x** - Producer, Workers, and Scripts
- **Apache Spark 3.5.0** - Stream processing
- **Delta Lake 3.1.0** - Storage format
- **RedPanda** - Kafka-compatible message broker
- **DuckDB** - In-process OLAP database
- **MongoDB** - NoSQL database for event logs
- **FastAPI & Uvicorn** - REST API framework
- **Discord Webhooks** - Alerting channel

**Frontend:**

- **Next.js 15+ & React** - UI Framework
- **Tailwind CSS v4** - Styling
- **Lightweight Charts** - Financial data visualization
- **Yarn** - Package manager

## Next Steps

- **Cloud Deployment:** Migrate local Docker infrastructure to cloud services (e.g., AWS MSK, Databricks, or GCP Dataproc).
- **AI Integration:** Implement a conversational AI assistant to analyze the Gold layer trends and suggest market insights in real-time.
- **Alerting System:** Add webhooks or email alerts when high price volatility is detected within a 1-minute window.
