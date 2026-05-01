# Лабораторная работа 2 — Spark ETL (MAI BigData)

ETL-пайплайн на Apache Spark: сырые CSV (10 файлов) -> staging в PostgreSQL ->
звёздная схема в PostgreSQL -> 6 аналитических витрин в ClickHouse.

## Архитектура

- **PostgreSQL** (`mai_postgres`, порт `5434`) — staging-таблица `staging_data`
  и звёздная схема (`customer_dim`, `seller_dim`, `product_dim`, `store_dim`,
  `supplier_dim`, `calendar_dim`, `sales_fact`).
- **ClickHouse** (`mai_clickhouse`, порт HTTP `8124`, native `9001`) —
  6 витрин: `report_products`, `report_customers`, `report_timeline`,
  `report_stores`, `report_suppliers`, `report_quality`.
- **Spark 3.5.3** (`mai_spark`) — единый скрипт `pipeline.py`, две стадии:
  `build_star_schema()` и `build_reports()`.

Стиль реализации — **Spark SQL** (`createOrReplaceTempView` + `spark.sql(...)`),
без цепочек DataFrame API. Дедупликация измерений выполняется через
`ROW_NUMBER() OVER (PARTITION BY business_key)`.

## Запуск

```bash
docker compose up -d --build
```

Пайплайн выполнится автоматически при старте контейнера `mai_spark`
(скрипт `start.sh` ждёт ~30 секунд, затем вызывает `spark-submit`).
Полное выполнение занимает ~1-2 минуты.

Логи:
```bash
docker logs -f mai_spark
```

## Проверка результатов

PostgreSQL (звёздная схема):
```bash
docker exec -it mai_postgres psql -U student -d mai_lab2_db -c "\dt"
docker exec -it mai_postgres psql -U student -d mai_lab2_db -c "SELECT COUNT(*) FROM sales_fact;"
```

ClickHouse (витрины):
```bash
docker exec -it mai_clickhouse clickhouse-client --user click --password click \
    --query "SHOW TABLES"
docker exec -it mai_clickhouse clickhouse-client --user click --password click \
    --query "SELECT * FROM report_products LIMIT 5 FORMAT Pretty"
```

## Учётные данные

| Сервис      | Пользователь | Пароль        | База/Порт                |
|-------------|--------------|---------------|--------------------------|
| PostgreSQL  | `student`    | `bigdata2026` | `mai_lab2_db`, `5434`    |
| ClickHouse  | `click`      | `click`       | `default`, `8124`/`9001` |

## Остановка

```bash
docker compose down -v
```
