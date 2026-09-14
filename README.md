# IPCA Data Lakehouse

End-to-end data pipeline that captures historical Brazilian Treasury bond series (IPCA+ and Prefixado), streams them out of PostgreSQL via Kafka CDC, lands the data in Amazon S3 across bronze/silver/gold layers, and exposes the curated data for querying via Spark SQL.

## Architecture

```
                 ┌─────────────┐        ┌──────────────────┐
  Tesouro Direto │  Python ETL │──────▶ │   PostgreSQL      │
  (public API)   │  (importar) │        └────────┬──────────┘
                 └─────────────┘                 │
                                                  │ Kafka Connect
                                                  │ (JDBC Source)
                                                  ▼
                                          ┌──────────────┐
                                          │    Kafka     │
                                          │   Cluster    │
                                          └──────┬───────┘
                                                  │ Kafka Connect
                                                  │ (S3 Sink)
                                                  ▼
                                     ┌────────────────────────┐
                                     │   Amazon S3 (bronze)    │
                                     └───────────┬────────────┘
                                                  │ Spark ETL
                                                  ▼
                                     ┌────────────────────────┐
                                     │  S3 (silver / gold)     │
                                     └────────────────────────┘
```

**Components:**

| Service | Role |
|---|---|
| PostgreSQL | Stores the raw data collected from the Tesouro Direto API |
| Zookeeper + Kafka Broker | Messaging backbone for the pipeline |
| Schema Registry | Versions the Avro schemas of the Kafka messages |
| Kafka Connect (custom image) | JDBC Source (Postgres → Kafka) and S3 Sink (Kafka → S3) connectors |
| ksqlDB | Continuous queries over Kafka topics |
| REST Proxy | HTTP access to the Kafka cluster |
| Spark (master/worker/Jupyter) | ETL across bronze → silver → gold layers, plus SQL queries |

## Project structure

```
.
├── docker-compose.yaml              # Main stack: Kafka, Connect, Postgres, ksqlDB, REST Proxy
├── custom-kafka-connector-image/    # Kafka Connect image with the JDBC and S3 connectors
├── connectors/
│   ├── source/                      # JDBC connector configs (Postgres → Kafka)
│   └── sink/                        # S3 Sink connector configs (Kafka → S3)
├── postgres/
│   └── docker-compose.yaml          # Standalone PostgreSQL stack
├── spark/
│   ├── docker-compose.yaml          # Spark stack (master, worker, Jupyter)
│   └── jars/                        # S3 integration JARs (download manually, see below)
├── importar.ipynb                   # Ingestion: Tesouro Direto API → PostgreSQL
├── etl-spark.ipynb                  # ETL: S3 bronze → silver → gold
├── spark_sql_pipeline.ipynb         # Spark SQL queries over Postgres and Kafka
└── .env_kafka_connect.example       # Environment variable template (AWS credentials)
```

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- An AWS account with an S3 bucket and an IAM user with read/write access to that bucket

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/EduardoRamilloLeite/ipca-data-lakehouse.git
cd ipca-data-lakehouse
```

### 2. Create the Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pandas sqlalchemy psycopg2-binary python-dotenv pyspark boto3
```

### 3. Configure AWS credentials

Copy the example file and fill in your own credentials. **Never commit the real `.env_kafka_connect`** — it is already listed in `.gitignore`.

```bash
cp .env_kafka_connect.example .env_kafka_connect
```

```
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

### 4. Download the Spark ↔ S3 integration JARs

These files exceed GitHub's file size limit and are not part of the repository. Download them and place a copy in the project root and in `spark/jars/`:

- [`hadoop-aws-3.3.4.jar`](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar)
- [`aws-java-sdk-bundle-1.12.262.jar`](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar)

### 5. Build the custom Kafka Connect image

```bash
cd custom-kafka-connector-image
docker build -t connect-custom:1.0.0 .
cd ..
```

### 6. Start the main stack

```bash
docker compose up -d
```

### 7. Register the connectors

```bash
curl -X POST -H "Content-Type: application/json" --data @connectors/source/connect_jdbc_postgres_ipca.config http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/source/connect_jdbc_postgres_pre.config http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/sink/connect_s3_sink_ipca.config http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/sink/connect_s3_sink_pre.config http://localhost:8083/connectors
```

### 8. Start the Spark stack

```bash
cd spark
docker compose up -d
cd ..
```

### 9. Run the pipeline

1. `importar.ipynb` — collects the public Tesouro Direto data and writes it to PostgreSQL.
2. The connectors automatically replicate PostgreSQL to Kafka, and Kafka to S3 (bronze layer).
3. `etl-spark.ipynb` — reads the bronze layer, applies cleanup/transformation (silver) and aggregations (gold).
4. `spark_sql_pipeline.ipynb` — exploratory SQL queries over PostgreSQL and Kafka.

## Exposed ports

| Service | Port |
|---|---|
| Kafka Broker | 9092 |
| Schema Registry | 8081 |
| Kafka Connect (REST API) | 8083 |
| REST Proxy | 8082 |
| ksqlDB Server | 8088 |
| PostgreSQL | 5432 |
| Spark Master UI | 8080 |
| Spark Worker UI | 8090 |
| Jupyter Notebook | 8888 |

## Security

- AWS credentials live in `.env_kafka_connect`, which is git-ignored — use `.env_kafka_connect.example` as a reference.
- The PostgreSQL credentials used (`postgres`/`postgres`) are local/study-environment defaults and should not be reused in production.
- The IAM user tied to this project should be scoped to only the buckets used by the pipeline (see the policies under `connectors/sink`).
