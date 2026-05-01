# Лабораторная работа №1 — схема «снежинка» (зоомагазин)

Хранилище данных по продажам товаров для домашних животных.
СУБД — PostgreSQL 16, всё разворачивается одной командой `docker compose up -d`.

## Запуск

```bash
cd mai-bigdata-labs/lab1
docker compose up -d
```

После старта контейнер `mai_lab1_db` автоматически выполнит два скрипта
из `sql/`:

1. `01_load_and_stage.sql` — создаёт промежуточную таблицу `staging_raw`
   и загружает в неё все 10 CSV-файлов из `data/` (10 000 строк).
2. `02_build_warehouse.sql` — создаёт все таблицы «снежинки»
   (поддименции, измерения, факт), наполняет их и строит индексы.

Параметры подключения:

| Параметр | Значение      |
|----------|---------------|
| host     | localhost     |
| port     | 5433          |
| db       | mai_warehouse |
| user     | student       |
| password | bigdata2026   |

## Подключение и проверка

```bash
docker exec -it mai_lab1_db psql -U student -d mai_warehouse
```

Количество строк во всех таблицах:

```sql
select 'staging_raw'  as t, count(*) from staging_raw
union all select 'fact_sales',     count(*) from fact_sales
union all select 'dim_customer',   count(*) from dim_customer
union all select 'dim_seller',     count(*) from dim_seller
union all select 'dim_product',    count(*) from dim_product
union all select 'dim_store',      count(*) from dim_store
union all select 'dim_supplier',   count(*) from dim_supplier
union all select 'dim_calendar',   count(*) from dim_calendar;
```

## Схема

```
fact_sales
  -> dim_customer  -> dim_address_customer, dim_pet_info
  -> dim_seller    -> dim_address_seller
  -> dim_product   -> dim_category_product, dim_attributes_product
  -> dim_store     -> dim_address_store
  -> dim_supplier  -> dim_address_supplier
  -> dim_calendar
```

Идентификаторы из CSV игнорируются (повторяются между файлами); строки
объединяются по содержательным полям:

* клиент   — `customer_email`
* продавец — `seller_email`
* товар    — `(product_name, product_category, product_brand)`
* магазин  — `(store_name, store_city, store_country)`
* поставщик— `supplier_email`
* дата     — `sale_date` (формат `M/D/YYYY`).

## Остановка и удаление

```bash
docker compose down -v
```
