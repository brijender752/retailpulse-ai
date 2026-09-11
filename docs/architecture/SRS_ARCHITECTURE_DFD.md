# RetailPulse AI — SRS architecture and data flow diagrams

Version 1.0 | Repository baseline: 11 September 2026

## 1. Scope and notation

This document describes the implementation present in the repository and a separately identified target extension for BI, ML, and GenAI. “Implemented” means code/configuration exists; it does not assert that services are currently running or that end-to-end acceptance tests passed.

The system boundary includes the generator, operational database, ingestion, processing, storage, analytics, and orchestration. External actors are the operator and analytics consumer. Real storefront/customer integrations are future source systems; the current source workload is generated in Python.

In architecture diagrams, solid arrows carry data or query results, and dotted arrows carry control or explicitly labeled planned flows. In DFDs, rectangles are external entities, rounded nodes are numbered processes, cylinders are logical stores, and all arrows identify information being transferred. These are Mermaid representations of DFD notation, rather than strict graphical Yourdon symbols. Stores shown separately can share the same physical MinIO instance.

## 2. Layered implementation architecture

```mermaid
flowchart TB
  subgraph L1["1. Source and operational layer"]
    GEN["Python data generator"]
    PG[("PostgreSQL 16 / ecommerce schema")]
    GEN -->|"SQL inserts and transactional workload"| PG
  end
  subgraph L2["2. Change capture and transport"]
    DBZ["Debezium 3.3.2 / Kafka Connect"]
    K[("Kafka 4.0.1 / KRaft / nine CDC topics")]
    PG -->|"Initial snapshot and logical WAL changes"| DBZ
    DBZ -->|"JSON CDC envelopes"| K
  end
  subgraph L3["3. Continuous processing / Flink 2.1.0 / PyFlink"]
    FB["Bronze CDC job"]
    FS["Silver normalization job"]
    FG["Gold jobs: orders, payments, products, Customer 360"]
    K -->|"Independent topic subscriptions"| FB
    K -->|"Independent topic subscriptions"| FS
    K -->|"Selected topic subscriptions"| FG
  end
  subgraph L4["4. Object storage / MinIO / Parquet"]
    B[("bronze/entity/")]
    S[("silver_stream/entity/")]
    G[("gold_stream/dataset/")]
    CP[("Customer 360 checkpoints and savepoints")]
    FB -->|"CDC rows and metadata"| B
    FS -->|"Normalized change records"| S
    FG -->|"Business output records"| G
    FG -->|"Recovery state"| CP
    CP -->|"Restored state"| FG
  end
  subgraph L5["5. Lakehouse processing / Spark 4.0.1 / Iceberg"]
    INC["Incremental CDC MERGE / five-minute DAG"]
    IS[("retailpulse.silver / current entity state")]
    WM[("retailpulse.control.cdc_watermarks")]
    GM["Spark Gold builds"]
    IG[("retailpulse.gold / business tables")]
    B -->|"Finalized CDC Parquet"| INC
    WM -->|"Previous timestamp per entity"| INC
    INC -->|"Committed progress"| WM
    INC -->|"Insert, update, delete"| IS
    IS -->|"Current rows"| GM
    GM -->|"Replace business tables"| IG
  end
  subgraph L6["6. Analytics transformation and SQL access"]
    DBT["dbt Core + dbt-spark / ephemeral staging / tests"]
    TH["Spark Thrift Server / port 10000"]
    AN[("Iceberg analytics tables / dimensions, facts, customer value")]
    DBT -->|"Compiled SQL through Thrift"| TH
    IS -->|"Source rows"| TH
    TH -->|"Materialized Iceberg tables"| AN
    TH -->|"Execution and test results"| DBT
    AN -->|"Query data"| TH
  end
  AF["Airflow 3.3.1 / LocalExecutor / health, submission, validation"]
  AF -.->|"Start and inspect jobs"| FB
  AF -.->|"Start and inspect jobs"| FS
  AF -.->|"Start, restore, inspect checkpoints"| FG
  AF -.->|"Submit Spark jobs"| INC
  AF -.->|"Submit builds and validation"| GM
  AF -.->|"Manual analytics DAG"| DBT
```

All Iceberg table data and Hadoop catalog metadata live under `s3a://retailpulse/warehouse` in MinIO. Iceberg is a table format/catalog integration, not a separate database server. Spark Thrift executes SQL; dbt compiles transformations and issues SQL through that service. The profile selects `analytics`, and model custom schemas also specify `analytics`; standard dbt schema generation can therefore resolve marts to `analytics_analytics`. Confirm the resolved schema in compiled artifacts before documenting a deployed SQL name.

The diagram above covers the recurring data path. The bootstrap and older bounded batch path are shown separately below because their storage and scheduling semantics differ.

## 3. DFD Level 0 — system context

```mermaid
flowchart LR
  OP["E1: Data engineer / platform operator"]
  P0("0: RetailPulse data platform")
  CON["E2: Analytics consumer / SQL client"]
  OP -->|"Generation settings, pipeline configuration, run requests"| P0
  P0 -->|"Run status, health, validation results, logs"| OP
  CON -->|"SQL analytical queries"| P0
  P0 -->|"Customer, order, payment and product results"| CON
```

The SQL consumer is an interface role; a dedicated dashboard application is not yet configured. Generation happens inside this boundary, so no external production storefront is implied.

## 4. DFD Level 1 — major processes and stores

```mermaid
flowchart TB
  E1["E1: Operator"]
  E2["E2: SQL consumer"]
  P1("1.0 Generate source workload / Python")
  D1[("D1: PostgreSQL ecommerce tables")]
  P2("2.0 Capture changes / Debezium")
  D2[("D2: Kafka CDC topics")]
  P3("3.0 Process streams / Flink")
  D3[("D3: Bronze CDC Parquet")]
  D4[("D4: Silver and Gold streaming Parquet")]
  D5[("D5: Flink recovery state")]
  P4("4.0 Build lakehouse / Spark + Iceberg")
  D6[("D6: Iceberg Bronze, Silver and Gold")]
  D7[("D7: CDC watermarks")]
  P5("5.0 Build analytics / dbt + Spark Thrift")
  D8[("D8: Iceberg analytics marts")]
  P6("6.0 Serve SQL / Spark Thrift")
  P7("7.0 Orchestrate and validate / Airflow")
  D9[("D9: Airflow metadata and task logs")]
  P8("8.0 Bounded batch / PySpark")
  D10[("D10: Batch Bronze, Silver and Gold Parquet")]
  E1 -->|"Generation configuration"| P1
  P1 -->|"Business records"| D1
  D1 -->|"Snapshot and WAL"| P2
  P2 -->|"CDC messages"| D2
  D2 -->|"Entity change streams"| P3
  P3 -->|"Historical change rows"| D3
  P3 -->|"Normalized and analytical rows"| D4
  P3 -->|"Offsets and operator state"| D5
  D5 -->|"Recovery state"| P3
  D3 -->|"CDC history and incremental candidates"| P4
  D4 -->|"Optional Gold bootstrap rows"| P4
  D6 -->|"Existing rows and snapshots"| P4
  P4 -->|"Bootstrap tables and refreshed current-state marts"| D6
  D7 -->|"Previous CDC progress"| P4
  P4 -->|"New CDC progress"| D7
  D6 -->|"Silver source rows"| P5
  P5 -->|"Dimensions, facts, business mart"| D8
  D8 -->|"Analytical rows"| P6
  D6 -->|"Lakehouse rows"| P6
  E2 -->|"SQL queries"| P6
  P6 -->|"Result sets"| E2
  E1 -->|"Pipeline settings and run requests"| P7
  P7 -->|"Run outcomes and validation results"| E1
  P7 -->|"Run and task records"| D9
  D9 -->|"Run history and configuration"| P7
  P7 -->|"Job submission and recovery parameters"| P3
  P3 -->|"Job and checkpoint status"| P7
  P7 -->|"Spark job parameters"| P4
  P4 -->|"Build and validation results"| P7
  P7 -->|"dbt commands"| P5
  P5 -->|"Test and documentation results"| P7
  E1 -->|"Manual batch run request"| P8
  D1 -->|"JDBC snapshot rows"| P8
  D10 -->|"Intermediate batch rows"| P8
  P8 -->|"Snapshot, cleaned and aggregate rows"| D10
  P8 -->|"Batch logs and counts"| E1
```

D10's Bronze prefix physically overlaps D3 in the current implementation. They are distinct logical datasets, not isolated physical locations. See the implementation constraints in section 12.

## 5. DFD Level 2 — streaming process 3.0

```mermaid
flowchart LR
  K[("D2: Nine Kafka CDC topics")]
  B("3.1 Decode and preserve CDC / Bronze job")
  S("3.2 Normalize CDC rows / Silver job")
  O("3.3 Build order summary")
  P("3.4 Build payment summary")
  R("3.5 Build product performance")
  C("3.6 Maintain customer state / Customer 360 recovery")
  DB[("D3: bronze/entity/")]
  DS[("D4a: silver_stream/entity/")]
  DG[("D4b: gold_stream/dataset/")]
  CP[("D5: Checkpoints and savepoints")]
  A["7.0: Airflow orchestration"]
  K -->|"All nine entities"| B
  K -->|"All nine entities"| S
  K -->|"Orders topic"| O
  K -->|"Orders and payments topics"| P
  K -->|"Products, orders and order_items topics"| R
  K -->|"Customers, orders, payments, website, support, marketing topics"| C
  B -->|"Rows with CDC metadata"| DB
  S -->|"Normalized change records"| DS
  O -->|"order_summary"| DG
  P -->|"order_payment_summary"| DG
  R -->|"product_performance"| DG
  C -->|"customer_360_recovery"| DG
  C -->|"Managed state and source offsets"| CP
  CP -->|"Recovery snapshot"| C
  A -->|"Submission and restore parameters for registered jobs"| C
  C -->|"Job and checkpoint status"| A
```

The six registered jobs are defined in `airflow/include/retailpulse/jobs.py`. Bronze, Silver, and Gold are output classifications: the active Silver job does not read Bronze files, and the active Gold jobs do not read Silver files. Their topic subscriptions are independent. Streaming files contain emitted records; they should not automatically be interpreted as one current row per business key.

## 6. DFD Level 2 — lakehouse process 4.0

```mermaid
flowchart TB
  B[("D3: Finalized Bronze CDC Parquet")]
  GS[("D4: Streaming Gold Parquet")]
  BO("4.1 One-time bootstrap / Spark")
  IB[("D6a: Iceberg Bronze")]
  BS("4.2 Bootstrap current-state Silver")
  RD("4.3 Read CDC and apply timestamp lookback")
  DD("4.4 Select latest event per entity key")
  ME("4.5 Apply guarded Iceberg MERGE")
  SI[("D6b: Iceberg Silver / nine entity tables")]
  WM[("D7: control.cdc_watermarks")]
  GB("4.6 Join and aggregate current Silver")
  GO[("D6c: Iceberg Gold")]
  V("4.7 Validate lakehouse outputs")
  A["7.0: Airflow"]
  B -->|"Initial CDC history"| BO
  GS -->|"Optional existing business outputs"| BO
  BO -->|"Copied CDC tables"| IB
  BO -->|"Optional copied Gold tables"| GO
  IB -->|"Latest event candidates"| BS
  BS -->|"Initial current-state rows"| SI
  B -->|"Recursive finalized Parquet read"| RD
  WM -->|"last_ts_ms minus five-minute lookback"| RD
  RD -->|"Eligible CDC rows"| DD
  DD -->|"Latest row per primary key"| ME
  SI -->|"Existing key and CDC timestamp"| ME
  ME -->|"r/c/u insert or update; d delete"| SI
  ME -->|"Successful merge maximum timestamp"| WM
  SI -->|"Current entity records"| GB
  GB -->|"order_summary, product_performance, customer_360"| GO
  SI -->|"Rows to validate"| V
  GO -->|"Rows to validate"| V
  V -->|"Validation outcome"| A
  A -->|"Job parameters and run request"| RD
```

The recurring job reads MinIO Bronze files directly; it does not continuously refresh Iceberg Bronze first. It orders candidates by `_ts_ms`, then `_ingested_at` when available. Updates and deletes require an incoming timestamp at least as new as the existing Silver row. Watermarks advance after the entity merge succeeds. Gold tables are rebuilt with `createOrReplace`, rather than incrementally merged.

## 7. dbt analytical lineage

```mermaid
flowchart LR
  SIL[("Iceberg Silver")]
  C["stg_customers"]
  P["stg_products"]
  O["stg_orders"]
  I["stg_order_items"]
  PAY["stg_payments"]
  W["stg_website_events"]
  S["stg_support_tickets"]
  M["stg_marketing_events"]
  DC[("dim_customer")]
  DP[("dim_product")]
  DT[("dim_date")]
  FO[("fact_orders")]
  FP[("fact_payments")]
  FA[("fact_customer_activity")]
  MV[("mart_customer_value")]
  SIL --> C
  SIL --> P
  SIL --> O
  SIL --> I
  SIL --> PAY
  SIL --> W
  SIL --> S
  SIL --> M
  C --> DC
  P --> DP
  O --> DT
  O --> FO
  I --> FO
  PAY --> FO
  O --> FP
  PAY --> FP
  C --> FA
  W --> FA
  S --> FA
  M --> FA
  DC --> MV
  FO --> MV
  FP --> MV
  FA --> MV
```

Arrows here are dbt model dependencies. All eight staging models are ephemeral SQL, not stored staging tables. Inventory is declared as a ninth Silver source but currently has no staging model or downstream mart dependency.

| Analytical object | Grain | Main information |
|---|---|---|
| `dim_customer` | One customer | Identity, location, source segment |
| `dim_product` | One product | Catalog, pricing, margin, inventory quantity |
| `dim_date` | One date over the order-date range | Calendar attributes |
| `fact_orders` | One order | Items, order value, payments, reconciliation |
| `fact_payments` | One payment | Amount, payment status, associated order |
| `fact_customer_activity` | One customer | Website, sessions, support and marketing aggregates |
| `mart_customer_value` | One customer | Lifetime order value, payments, activity, value segment |

The current schema has customer/date relationships for order analytics; there is no materialized order-line fact connecting `dim_product` to `fact_orders`. Customer value segmentation is SQL business logic, not a trained ML prediction.

## 8. Bounded batch flow

```mermaid
flowchart LR
  PG[("PostgreSQL ecommerce tables")]
  EX["PySpark JDBC extraction"]
  B[("MinIO bronze/entity / snapshot Parquet")]
  CL["PySpark Bronze-to-Silver cleanup"]
  S[("MinIO silver/entity / Parquet")]
  AG["PySpark Silver-to-Gold aggregation"]
  G[("MinIO gold/dataset / Parquet")]
  PG -->|"JDBC table snapshots"| EX
  EX -->|"Overwrite with ingestion timestamp"| B
  B -->|"Raw snapshot rows"| CL
  CL -->|"Cleaned rows"| S
  S -->|"Business entities"| AG
  AG -->|"Business aggregates"| G
```

These modules live under `spark/jobs/batch/` and are manually invoked in the runbook. They are separate from the Spark jobs under `lakehouse/iceberg/jobs/` that Airflow submits.

## 9. Tools and interfaces at each layer

| Layer | Tools configured in repository | Input → output | Interface / persistence |
|---|---|---|---|
| Source simulation | Python, generator modules | Generation settings → synthetic retail records | PostgreSQL client, SQL inserts |
| Operational storage | PostgreSQL 16 | Transactions → relational rows and WAL | SQL/JDBC, port 5432; `postgres_data` |
| CDC | Debezium 3.3.2, Kafka Connect, PostgreSQL connector | Snapshot/WAL → CDC envelopes | Connect REST 8083; Kafka internal config/offset/status topics |
| Event transport | Kafka 4.0.1, KRaft | CDC messages → independent consumer streams | Container 9092, host 9094; three partitions/topic, replication factor one |
| Streaming | Flink 2.1.0, Java 17, PyFlink DataStream/Table APIs | Kafka events → Bronze/Silver/Gold records | JobManager/TaskManager; REST/UI 8081 |
| Object lake | MinIO, Parquet | File sinks → persisted datasets | S3 API 9000, console 9001; `minio_data` |
| Bounded batch | Spark 4.0.1 / PySpark, PostgreSQL JDBC | Database snapshot → Bronze → Silver → Gold | JDBC and Hadoop S3A |
| Lakehouse | Spark, Iceberg 1.11.0 setting, Hadoop catalog | CDC files → current-state Silver and Gold tables | `s3a://retailpulse/warehouse`; data and metadata in MinIO |
| SQL execution | Spark Thrift Server, Iceberg extensions | SQL → reads, writes and result sets | Thrift port 10000 |
| Analytics modeling | dbt Core, dbt-spark, SQL, Jinja | Silver → eight staging expressions → seven marts | Thrift adapter; Iceberg table materialization |
| Data quality | dbt tests, Spark validation scripts, Airflow checks | Sources/models/service state → pass/fail and logs | Uniqueness, nullability, relationships, amounts, CDC timestamps |
| Orchestration | Airflow 3.3.1, LocalExecutor, Python Docker SDK | Schedules/run requests → submitted jobs and status | UI host 8090; local Docker socket, REST, S3 checks |
| Orchestration state | Separate PostgreSQL 16 `airflow-db` | Task/run metadata → persisted orchestration history | Internal Airflow network; log mount |
| Inspection | Kafka UI, Flink UI, MinIO console, Airflow UI | Platform metadata → operator views | Host ports 8080, 8081, 9001, 8090 |
| Packaging | Docker, Docker Compose, Python requirements | Images/configuration → local services | RetailPulse bridge network plus Airflow internal network |
| Future consumption | Superset, FastAPI, ML, RAG/LLM, Prometheus/Grafana | Planned analytics/telemetry → dashboards, predictions, answers | No implemented end-to-end serving flow found |

Versions above are repository declarations, not a claim about the latest upstream releases or a running deployment.

## 10. Entity and storage dictionary

For every entity below, the operational source is `ecommerce.<entity>`, its Kafka topic is `retailpulse.ecommerce.<entity>`, its CDC landing path is `s3://retailpulse/bronze/<entity>/`, and its current-state Iceberg table is `retailpulse.silver.<entity>`.

| Entity | Primary key | Business content |
|---|---|---|
| customers | customer_id | Identity, contact, geography, customer segment |
| products | product_id | Catalog, price, cost and inventory quantity |
| orders | order_id | Customer purchases, status and totals |
| order_items | order_item_id | Product quantities, unit prices and discounts |
| payments | payment_id | Order/customer payments and payment status |
| inventory | inventory_id | Inventory records |
| website_events | event_id | Customer browsing and session events |
| support_tickets | ticket_id | Customer support cases |
| marketing_events | marketing_event_id | Impressions, clicks, conversions and cost |

| Store | Location / content |
|---|---|
| Streaming Silver | `s3://retailpulse/silver_stream/<entity>/` |
| Streaming Gold | `gold_stream/order_summary/`, `order_payment_summary/`, `product_performance/`, `customer_360_recovery/` in the retailpulse bucket |
| Customer 360 recovery | `flink-checkpoints/customer360-recovery/` and `flink-savepoints/customer360-recovery/` |
| Default Flink recovery | Shared Docker volume mounted at `/tmp/flink-checkpoints`; specific jobs override storage |
| Iceberg Bronze | `retailpulse.bronze.<entity>`; one-time imported CDC history |
| Iceberg Silver | `retailpulse.silver.<entity>`; current entity state |
| Recurring Iceberg Gold | `retailpulse.gold.order_summary`, `.product_performance`, `.customer_360` |
| Optional bootstrap-only Gold | `retailpulse.gold.order_payment_summary` is also importable; the recurring Gold builder does not refresh it |
| Incremental progress | `retailpulse.control.cdc_watermarks`: table_name, last_ts_ms, updated_at |
| Analytics | Seven Iceberg marts; resolve actual namespace from dbt compilation |
| dbt artifacts | `dbt/target/`: compiled SQL, run/manifest and generated documentation artifacts when commands succeed |

## 11. Orchestration and deployment

```mermaid
flowchart TB
  OP["Operator"]
  subgraph AF["Airflow Compose stack"]
    API["API server / UI :8090"]
    SCH["Scheduler / LocalExecutor"]
    DP["DAG processor"]
    TR["Triggerer"]
    MDB[("airflow-db / PostgreSQL")]
    LOG[("Task logs")]
    DP -->|"Parsed DAG metadata"| MDB
    SCH -->|"Run and task state"| MDB
    MDB -->|"Scheduling metadata"| SCH
    API -->|"Metadata queries"| MDB
    TR -->|"Trigger state"| MDB
    SCH -->|"Task logs"| LOG
  end
  subgraph RP["RetailPulse Docker bridge network"]
    PG["PostgreSQL"]
    DEB["Debezium"]
    K["Kafka + Kafka UI"]
    JM["Flink JobManager"]
    TM["Flink TaskManager"]
    SP["spark-iceberg / spark-submit"]
    DB["dbt container"]
    TH["spark-thrift"]
    MI[("MinIO")]
  end
  OP -->|"Run requests and inspection"| API
  SCH -.->|"Docker SDK: submit PyFlink; REST: inspect"| JM
  JM -.->|"Task deployment"| TM
  SCH -.->|"Docker SDK: spark-submit"| SP
  SCH -.->|"Docker SDK: dbt commands"| DB
  DB -->|"SQL via Thrift"| TH
  TH -->|"S3A reads and writes"| MI
  SP -->|"S3A reads and writes"| MI
  TM -->|"S3 file output and recovery"| MI
  SCH -.->|"Health checks"| PG
  SCH -.->|"Connector status"| DEB
  SCH -.->|"Connectivity checks"| K
  SCH -.->|"Bucket and output checks"| MI
```

| DAG | Configured schedule | Workflow |
|---|---|---|
| `retailpulse_platform_health` | Every five minutes | PostgreSQL, Debezium, Kafka, Flink, MinIO health checks |
| `retailpulse_streaming_controller` | Every five minutes | Health checks → ensure registered Flink jobs → Customer 360 checkpoint/output checks |
| `retailpulse_iceberg_pipeline` | Manual | Initial lakehouse/bootstrap and Silver validation workflow |
| `retailpulse_iceberg_incremental` | Every five minutes | Flink/MinIO health → CDC MERGE → Gold rebuild → validation |
| `retailpulse_iceberg_maintenance` | Daily 02:00 UTC | Rewrite data files and manifests for configured Silver/Gold tables |
| `retailpulse_dbt_analytics` | Manual | `dbt debug` → `dbt build` → `dbt docs generate` |

Schedules are declarations; actual execution also depends on DAG availability, activation and platform health. The dbt DAG is not currently triggered automatically by completion of the incremental Iceberg DAG. Airflow stores orchestration metadata in its own PostgreSQL instance, separate from retail transactions. The local Docker-socket submission design is the implemented deployment; Kubernetes/Flink Operator is a future migration in the runbook.

## 12. Implementation constraints relevant to the SRS

1. **Shared Bronze destination:** the bounded JDBC extractor overwrites the same `bronze/<entity>` prefixes used by CDC and adds `_ingestion_timestamp`, whereas incremental CDC requires `_op` and `_ts_ms`. These paths must be isolated before treating batch extraction and continuous CDC as safely concurrent workflows.
2. **Separate freshness:** five-minute lakehouse scheduling does not imply five-minute analytics freshness because dbt is manually triggered. Schedule intervals are not measured latency guarantees.
3. **Current-state semantics:** streaming outputs and bootstrap copies are distinct from recurring Iceberg Silver current state. The recurring Gold builder replaces three tables from Silver; bootstrap can import a fourth Gold dataset.
4. **Timestamp replay limits:** the incremental job has a five-minute timestamp lookback, not an unlimited late-event guarantee. Physical deletes do not retain a persistent per-key tombstone/version ledger, so replay correctness after deletes requires additional acceptance testing.
5. **Schema resolution:** the dbt profile and custom model schema can combine into `analytics_analytics`. The intended `analytics` name should be reconciled with compiled/deployed relations.
6. **Quality scope:** existing tests and validation are present, but no dedicated quarantine/DLQ flow, universal quality gate before every write, or measured SLO is established here.
7. **Maintenance scope:** the maintenance script rewrites data files/manifests for configured Silver and Gold tables; it does not currently cover dbt analytics tables or implement snapshot expiry.
8. **Deployment scope:** this is a local Compose topology with one Kafka broker and replication factor one. The document does not claim production high availability or a deployed BI/AI service.

## 13. Target extension — planned BI, ML and GenAI

This is a proposed completion of the project vision in the README. Dotted arrows below are planned integrations, and the serving/ML tools and contracts still require implementation decisions.

```mermaid
flowchart LR
  AN[("Existing Iceberg analytics and Gold")]
  SQL["Existing Spark SQL / Thrift"]
  BI["PLANNED: Superset dashboards"]
  ML["PLANNED: ML training and inference"]
  PRED[("PLANNED: Prediction outputs")]
  API["PLANNED: FastAPI serving"]
  RAG["PLANNED: GenAI analyst / retrieval and LLM"]
  META[("PLANNED: Searchable semantic context / model documentation")]
  U["Business user"]
  TEL["Platform telemetry"]
  PROM["PLANNED: Prometheus"]
  GRAF["PLANNED: Grafana"]
  AN -->|"Query data"| SQL
  SQL -.->|"KPI result sets"| BI
  BI -.->|"Dashboards"| U
  AN -.->|"Training and scoring features"| ML
  ML -.->|"Scores and predictions"| PRED
  PRED -.->|"Prediction results"| API
  U -.->|"Natural-language question"| API
  API -.->|"Question and context"| RAG
  META -.->|"Definitions and retrieval context"| RAG
  RAG -.->|"Analytical SQL request"| SQL
  SQL -.->|"Grounding result set"| RAG
  RAG -.->|"Grounded answer"| API
  API -.->|"Answer or prediction response"| U
  TEL -.->|"Exported metrics"| PROM
  PROM -.->|"Time series"| GRAF
  GRAF -.->|"Operational dashboards"| U
```

## 14. Repository traceability

| Diagram / concern | Implementation evidence |
|---|---|
| Service topology and ports | [Root Compose](../../docker-compose.yml), [Airflow Compose](../../airflow/docker-compose.airflow.yml) |
| Source workload | [Generator](../../data_generator/main.py), [DDL](../../sql/02_create_tables.sql) |
| Active stream membership | [Flink job registry](../../airflow/include/retailpulse/jobs.py) |
| Streaming transformations | [Bronze](../../flink/jobs/streaming/bronze/retailpulse_bronze.py), [Silver](../../flink/jobs/streaming/silver/retailpulse_streaming_silver.py), [Gold directory](../../flink/jobs/streaming/gold/) |
| Batch flow | [Batch modules](../../spark/jobs/batch/) |
| Iceberg catalog and bootstrap | [Session](../../lakehouse/iceberg/jobs/iceberg_session.py), [Bootstrap](../../lakehouse/iceberg/jobs/bootstrap_minio_to_iceberg.py) |
| Incremental merge | [CDC to Silver](../../lakehouse/iceberg/jobs/incremental_cdc_to_silver.py) |
| Iceberg business tables | [Gold builder](../../lakehouse/iceberg/jobs/build_gold_marts.py) |
| Analytics source and output definitions | [Sources](../../dbt/models/sources.yml), [Project](../../dbt/dbt_project.yml), [Profile](../../dbt/profiles.yml), [Models](../../dbt/models/) |
| Orchestration schedules | [DAGs](../../airflow/dags/) |
| Data quality | [dbt tests](../../dbt/tests/), [Model tests](../../dbt/models/marts/schema.yml), [Iceberg validation](../../lakehouse/iceberg/jobs/validate_incremental_pipeline.py) |

The older README phase labels lag behind the current implementation. This document gives precedence to active job registrations, DAGs, model SQL and Compose configuration.
