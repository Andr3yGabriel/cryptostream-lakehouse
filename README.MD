 # CryptoStream Lakehouse

## Project Scope and Goal

CryptoStream Lakehouse is an end-to-end data pipeline project that ingests near‑real‑time cryptocurrency quotes, computes market indicators with Spark Structured Streaming, and makes the data available for both historical and live analytics.

The goal is to demonstrate a complete streaming lakehouse architecture with clear data layers (Bronze, Silver, Gold), strict schema enforcement, and performance/resilience best practices.

## Scope

### Data Sources
- Public CoinGecko API (free tier) or Binance API.
- Top 10 cryptocurrencies by market cap.
- Data extracted per asset:
	- Trading pair (e.g., BTC-USD, ETH-USD, SOL-USD)
	- Current price
	- 24h trading volume
	- Collection timestamp

### Functional Requirements
- **Continuous ingestion (Producer):** a service (Python or Node.js) queries the API every 10–15 seconds and publishes messages to a streaming topic.
- **Streaming processing (Consumer):** Databricks / Spark reads the stream in real time.
- **Windowed aggregations:** compute a 5‑minute sliding moving average for price using Spark time window functions.
- **Lakehouse layers:**
	- **Bronze:** raw events as they arrive.
	- **Silver:** cleaned and typed data, deduplicated and enriched with date/time columns (year, month, day, hour).
	- **Gold:** aggregated tables for analytics (e.g., average price per hour).
- **Schema enforcement:** incompatible data types should not break the pipeline; invalid events should be handled safely (e.g., routed to a dead‑letter path).

### Non‑Functional Requirements
- **Latency:** Silver availability in under 1 minute (micro‑batching).
- **Infrastructure as Code:** local environment (Producer + Kafka) starts with a single `docker-compose up`.
- **Resilience:** Spark jobs use checkpointing to resume exactly where they stopped.
- **Scalability:** storage in Delta Lake (optimized Parquet), partitioned by date.

### Data Visualization Goals
- Suggested tools: Power BI (Databricks SQL Warehouse) or Streamlit.
- **Chart:** price vs. 5‑minute moving average (live line chart).
- **KPIs:** hourly percentage change (e.g., Bitcoin ▲ 1.2%).
- **Table:** latest ingested transactions.

## Delivery Plan (4 Weekly Sprints)

### Week 1 — Foundation and Ingestion
- Set up the repository and local Kafka stack via Docker Compose.
- Build the Producer to fetch quotes and publish to the `crypto-prices` topic.
- Evidence: terminal output showing messages arriving in Kafka.

### Week 2 — Streaming in Databricks
- Connect Databricks to Kafka.
- Create notebook `01_Bronze_Ingestion` to stream into the Bronze Delta table.
- Evidence: continuous inserts visible in a Bronze table query.

### Week 3 — Transformation and Quality (Silver & Gold)
- Create notebook `02_Silver_Transformation` for parsing, typing, deduplication, and enrichment.
- Create notebook `03_Gold_Analytics` for moving averages and time aggregations.
- Apply partitioning to output tables.
- Evidence: performant analytical queries on final tables.

### Week 4 — Orchestration and Visualization
- Schedule notebooks as jobs (or run end‑to‑end manually if limited by Community Edition).
- Connect Power BI or build a Streamlit dashboard.
- Produce the requested visualizations.
- Final documentation with architecture diagram and run instructions.
