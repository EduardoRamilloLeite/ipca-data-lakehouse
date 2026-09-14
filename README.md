# IPCA Data Lakehouse

Pipeline de dados end-to-end que captura séries históricas de títulos do Tesouro Direto (IPCA+ e Prefixado), publica no Kafka via CDC do PostgreSQL, persiste no Amazon S3 em camadas (bronze/silver/gold) e disponibiliza os dados tratados para consulta via Spark SQL.

## Arquitetura

```
                 ┌─────────────┐        ┌──────────────────┐
 Tesouro Direto  │  Python ETL │──────▶ │   PostgreSQL      │
  (API pública)  │ (importar)  │        └────────┬──────────┘
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

**Componentes:**

| Serviço | Papel |
|---|---|
| PostgreSQL | Armazena os dados brutos coletados da API do Tesouro Direto |
| Zookeeper + Kafka Broker | Backbone de mensageria do pipeline |
| Schema Registry | Versiona os schemas Avro das mensagens do Kafka |
| Kafka Connect (imagem custom) | Conectores JDBC Source (Postgres → Kafka) e S3 Sink (Kafka → S3) |
| ksqlDB | Consultas contínuas sobre os tópicos Kafka |
| REST Proxy | Acesso HTTP ao cluster Kafka |
| Spark (master/worker/Jupyter) | ETL das camadas bronze → silver → gold e consultas SQL |

## Estrutura do projeto

```
.
├── docker-compose.yaml              # Stack principal: Kafka, Connect, Postgres, ksqlDB, REST Proxy
├── custom-kafka-connector-image/    # Imagem do Kafka Connect com os conectores JDBC e S3
├── connectors/
│   ├── source/                      # Configs dos conectores JDBC (Postgres → Kafka)
│   └── sink/                        # Configs dos conectores S3 Sink (Kafka → S3)
├── postgres/
│   └── docker-compose.yaml          # Stack isolada do PostgreSQL
├── spark/
│   ├── docker-compose.yaml          # Stack do Spark (master, worker, Jupyter)
│   └── jars/                        # JARs de integração com S3 (baixar manualmente, ver abaixo)
├── importar.ipynb                   # Ingestão: API Tesouro Direto → PostgreSQL
├── etl-spark.ipynb                  # ETL: S3 bronze → silver → gold
├── spark_sql_pipeline.ipynb         # Consultas Spark SQL sobre Postgres e Kafka
└── .env_kafka_connect.example       # Modelo de variáveis de ambiente (credenciais AWS)
```

## Pré-requisitos

- Docker e Docker Compose
- Python 3.11+
- Uma conta AWS com um bucket S3 e um usuário IAM com permissão de leitura/escrita nesse bucket

## Configuração

### 1. Clonar o repositório

```bash
git clone https://github.com/EduardoRamilloLeite/ipca-data-lakehouse.git
cd ipca-data-lakehouse
```

### 2. Criar o ambiente virtual Python

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pandas sqlalchemy psycopg2-binary python-dotenv pyspark boto3
```

### 3. Configurar as credenciais AWS

Copie o arquivo de exemplo e preencha com suas próprias credenciais. **Nunca versione o `.env_kafka_connect` real** — ele já está no `.gitignore`.

```bash
cp .env_kafka_connect.example .env_kafka_connect
```

```
AWS_ACCESS_KEY_ID=sua-access-key
AWS_SECRET_ACCESS_KEY=sua-secret-key
```

### 4. Baixar os JARs de integração Spark ↔ S3

Esses arquivos passam do limite de tamanho do GitHub e por isso não fazem parte do repositório. Baixe e coloque na raiz do projeto e em `spark/jars/`:

- [`hadoop-aws-3.3.4.jar`](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar)
- [`aws-java-sdk-bundle-1.12.262.jar`](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar)

### 5. Construir a imagem customizada do Kafka Connect

```bash
cd custom-kafka-connector-image
docker build -t connect-custom:1.0.0 .
cd ..
```

### 6. Subir a stack principal

```bash
docker compose up -d
```

### 7. Registrar os conectores

```bash
curl -X POST -H "Content-Type: application/json" --data @connectors/source/connect_jdbc_postgres_ipca.config http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/source/connect_jdbc_postgres_pre.config http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/sink/connect_s3_sink_ipca.config http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @connectors/sink/connect_s3_sink_pre.config http://localhost:8083/connectors
```

### 8. Subir a stack do Spark

```bash
cd spark
docker compose up -d
cd ..
```

### 9. Rodar o pipeline

1. `importar.ipynb` — coleta os dados públicos do Tesouro Direto e grava no PostgreSQL.
2. Os conectores replicam o PostgreSQL para o Kafka e do Kafka para o S3 (camada bronze) automaticamente.
3. `etl-spark.ipynb` — lê a camada bronze, aplica limpeza/transformação (silver) e agregações (gold).
4. `spark_sql_pipeline.ipynb` — consultas SQL exploratórias sobre PostgreSQL e Kafka.

## Portas expostas

| Serviço | Porta |
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

## Segurança

- Credenciais AWS ficam em `.env_kafka_connect`, que é ignorado pelo Git — use `.env_kafka_connect.example` como referência.
- As credenciais de PostgreSQL usadas (`postgres`/`postgres`) são padrões de ambiente local/estudo e não devem ser reutilizadas em produção.
- O usuário IAM associado a este projeto deve ter permissão restrita apenas aos buckets utilizados pelo pipeline (ver policies em `connectors/sink`).
