# Banking Lakehouse Pipeline

Dự án mô phỏng pipeline dữ liệu ngân hàng theo kiến trúc lakehouse. Dữ liệu giao dịch được sinh giả lập vào PostgreSQL, capture thay đổi bằng Debezium, stream qua Kafka, lưu raw data dạng Parquet vào MinIO, nạp vào Snowflake, biến đổi bằng dbt và trực quan hóa bằng Power BI.

![Banking lakehouse architecture](images/Architecture.jpg)

## Mục tiêu

- Mô phỏng nguồn dữ liệu banking gồm `customers`, `accounts`, `transactions`.
- Xây dựng luồng CDC từ PostgreSQL sang Kafka bằng Debezium.
- Lưu dữ liệu raw theo dạng Parquet trên MinIO để mô phỏng data lake.
- Nạp dữ liệu raw vào Snowflake và tổ chức theo các lớp `raw`, `staging`, `marts`.
- Dùng dbt để tạo staging model, fact, dimension và snapshot SCD Type 2.
- Dùng Airflow để điều phối load dữ liệu và chạy dbt.

## Luồng xử lý

1. `data-generator/faker_generator.py` sinh dữ liệu giả lập vào PostgreSQL.
2. Debezium đọc WAL của PostgreSQL và publish CDC event vào Kafka topic:
   - `banking_server.public.customers`
   - `banking_server.public.accounts`
   - `banking_server.public.transactions`
3. `kafka/consumer/kafka_to_minio.py` consume Kafka event, gom batch và ghi Parquet vào MinIO bucket `raw`.
4. Airflow DAG `minio_to_snowflake_banking` tải Parquet từ MinIO và load vào các bảng raw trên Snowflake.
5. Airflow DAG `SCD2_snapshots` chạy `dbt snapshot` và `dbt run --select marts`.
6. Power BI đọc dữ liệu marts từ Snowflake để dựng dashboard.

## Tech Stack

| Thành phần | Công nghệ |
| --- | --- |
| Source database | PostgreSQL 15 |
| CDC | Debezium, Kafka Connect |
| Streaming | Apache Kafka, Zookeeper |
| Object storage | MinIO |
| Orchestration | Apache Airflow |
| Data warehouse | Snowflake |
| Transformation | dbt, dbt-snowflake |
| Visualization | Power BI |
| Data generator | Python, Faker |

## Cấu trúc thư mục

```text
.
+-- banking_dbt/                  # dbt project: staging, marts, snapshots
+-- data-generator/               # Script sinh dữ liệu banking giả lập
+-- docker/dags/                  # Airflow DAGs
+-- images/                       # Ảnh kiến trúc và dashboard
+-- kafka/consumer/               # Kafka consumer ghi dữ liệu vào MinIO
+-- kafka/kafka-debezium/         # Script tạo Debezium connector
+-- postgres/                     # SQL tạo bảng nguồn PostgreSQL
+-- docker-compose.yml            # Hạ tầng local
+-- dockerfile-airflow.dockerfile # Airflow image có dbt
+-- requirements.txt              # Python dependencies
```

## Điều kiện cần

- Docker và Docker Compose.
- Python 3.10+.
- Tài khoản Snowflake có quyền tạo database, schema, table và chạy warehouse.
- Power BI Desktop nếu muốn mở hoặc dựng dashboard.

## Cấu hình môi trường

Tạo các file `.env` từ mẫu:

```bash
cp .env.example .env
cp data-generator/.env.example data-generator/.env
cp kafka/kafka-debezium/.env.example kafka/kafka-debezium/.env
cp kafka/consumer/.env.example kafka/consumer/.env
cp docker/dags/.env.example docker/dags/.env
```

Kiểm tra lại các nhóm biến chính:

- `.env`: credential cho PostgreSQL, MinIO và Airflow metadata database.
- `data-generator/.env`: kết nối từ máy host vào PostgreSQL qua `localhost:5432`.
- `kafka/kafka-debezium/.env`: kết nối từ container Debezium vào PostgreSQL qua hostname `postgres`.
- `kafka/consumer/.env`: Kafka broker `localhost:29092`, MinIO endpoint `http://localhost:9000`.
- `docker/dags/.env`: credential MinIO nội bộ container và credential Snowflake.

Nếu dùng Linux, đặt `AIRFLOW_UID` trong `.env` bằng kết quả của lệnh `id -u`.

## Cài Python dependencies

Các script Debezium connector, Kafka consumer và data generator chạy từ máy host:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Khởi động hạ tầng local

Build Airflow image và chạy các service nền:

```bash
docker compose build
docker compose up -d postgres zookeeper kafka connect minio airflow-postgres
```

Khởi tạo metadata database và user cho Airflow:

```bash
docker compose run --rm --no-deps airflow-webserver airflow db migrate
docker compose run --rm --no-deps airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
docker compose up -d airflow-webserver airflow-scheduler
```

Các UI/service chính:

| Service | URL |
| --- | --- |
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Debezium Connect API | http://localhost:8083 |
| PostgreSQL | `localhost:5432` |
| Kafka broker từ host | `localhost:29092` |

## Tạo bảng nguồn PostgreSQL

Chạy DDL tạo 3 bảng nguồn:

```bash
docker compose exec -T postgres psql -U postgres -d banking < postgres/create_table.sql
```

Nếu đã đổi `POSTGRES_USER` hoặc `POSTGRES_DB` trong `.env`, thay lại tham số `-U` và `-d` cho khớp.

## Tạo Debezium connector

```bash
python kafka/kafka-debezium/create_debezium_connector.py
```

Connector sẽ theo dõi các bảng:

- `public.customers`
- `public.accounts`
- `public.transactions`

## Chạy pipeline streaming

Mở một terminal để chạy Kafka consumer:

```bash
source .venv/bin/activate
python kafka/consumer/kafka_to_minio.py
```

Mở terminal khác để sinh dữ liệu:

```bash
source .venv/bin/activate
python data-generator/faker_generator.py
```

Nếu chỉ muốn sinh một batch rồi dừng:

```bash
python data-generator/faker_generator.py --once
```

Consumer sẽ ghi file Parquet vào MinIO theo cấu trúc:

```text
s3://raw/customers/date=YYYY-MM-DD/*.parquet
s3://raw/accounts/date=YYYY-MM-DD/*.parquet
s3://raw/transactions/date=YYYY-MM-DD/*.parquet
```

## Chuẩn bị Snowflake

Tạo database, schema và raw tables để Airflow load Parquet:

```sql
CREATE DATABASE IF NOT EXISTS banking;
CREATE SCHEMA IF NOT EXISTS banking.raw;
CREATE SCHEMA IF NOT EXISTS banking.analytics;

CREATE TABLE IF NOT EXISTS banking.raw.customers (v VARIANT);
CREATE TABLE IF NOT EXISTS banking.raw.accounts (v VARIANT);
CREATE TABLE IF NOT EXISTS banking.raw.transactions (v VARIANT);
```

Cập nhật credential Snowflake trong:

- `docker/dags/.env`
- `banking_dbt/.dbt/profiles.yml`

Không commit credential thật vào repository. Với môi trường chia sẻ, nên dùng file example hoặc secret manager.

## Chạy Airflow và dbt

Truy cập Airflow tại http://localhost:8080 với user `admin` và password `admin`.

Bật và trigger các DAG theo thứ tự:

1. `minio_to_snowflake_banking`: load dữ liệu Parquet từ MinIO vào Snowflake raw tables.
2. `SCD2_snapshots`: chạy dbt snapshot và build marts.

Có thể kiểm tra dbt trực tiếp từ host:

```bash
cd banking_dbt
dbt debug --profiles-dir .dbt
dbt snapshot --profiles-dir .dbt
dbt run --select marts --profiles-dir .dbt
```

## dbt Models

| Layer | Model | Mục đích |
| --- | --- | --- |
| Staging | `stg_customer` | Chuẩn hóa customer raw event, lấy bản ghi mới nhất theo `customer_id` |
| Staging | `stg_account` | Chuẩn hóa account raw event, lấy bản ghi mới nhất theo `account_id` |
| Staging | `stg_transaction` | Chuẩn hóa transaction raw event |
| Snapshot | `customers_snapshot` | Theo dõi thay đổi customer theo SCD Type 2 |
| Snapshot | `accounts_snapshot` | Theo dõi thay đổi account theo SCD Type 2 |
| Mart | `dim_customer` | Dimension customer có hiệu lực theo thời gian |
| Mart | `dim_account` | Dimension account có hiệu lực theo thời gian |
| Mart | `fact_transaction` | Fact giao dịch, incremental theo `transaction_id` |

## Dashboard

Dashboard Power BI minh họa các chỉ số tổng quan như tổng khách hàng, tổng tài khoản, tổng giao dịch, số dư trung bình và phân bổ giao dịch theo loại.

![Power BI banking dashboard](images/Visualization.jpg)

## Dừng môi trường

```bash
docker compose down
```

Nếu muốn xóa cả dữ liệu volume/container state local:

```bash
docker compose down -v
```

## Troubleshooting

- Debezium không tạo connector: kiểm tra container `connect` đã chạy và endpoint `http://localhost:8083/connectors` sẵn sàng.
- Không có Kafka message: kiểm tra PostgreSQL đã bật logical replication trong `docker-compose.yml` và bảng nguồn đã có dữ liệu mới.
- Consumer không ghi MinIO: kiểm tra `kafka/consumer/.env`, bucket `raw` và MinIO credential.
- Airflow load Snowflake lỗi: kiểm tra raw tables đã tồn tại, warehouse đang chạy và credential trong `docker/dags/.env`.
- dbt lỗi profile: kiểm tra `banking_dbt/.dbt/profiles.yml`, account Snowflake, role, warehouse, database và schema.
