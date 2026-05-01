-- =====================================================================
-- 01_load_and_stage.sql
-- Создание промежуточной (staging) таблицы и загрузка всех CSV-файлов.
-- Все колонки храним как text — приведение типов выполняется на этапе
-- построения хранилища (см. 02_build_warehouse.sql).
-- =====================================================================

drop table if exists staging_raw cascade;

create table staging_raw (
    id                    text,
    customer_first_name   text,
    customer_last_name    text,
    customer_age          text,
    customer_email        text,
    customer_country      text,
    customer_postal_code  text,
    customer_pet_type     text,
    customer_pet_name     text,
    customer_pet_breed    text,
    seller_first_name     text,
    seller_last_name      text,
    seller_email          text,
    seller_country        text,
    seller_postal_code    text,
    product_name          text,
    product_category      text,
    product_price         text,
    product_quantity      text,
    sale_date             text,
    sale_customer_id      text,
    sale_seller_id        text,
    sale_product_id       text,
    sale_quantity         text,
    sale_total_price      text,
    store_name            text,
    store_location        text,
    store_city            text,
    store_state           text,
    store_country         text,
    store_phone           text,
    store_email           text,
    pet_category          text,
    product_weight        text,
    product_color         text,
    product_size          text,
    product_brand         text,
    product_material      text,
    product_description   text,
    product_rating        text,
    product_reviews       text,
    product_release_date  text,
    product_expiry_date   text,
    supplier_name         text,
    supplier_contact      text,
    supplier_email        text,
    supplier_phone        text,
    supplier_address      text,
    supplier_city         text,
    supplier_country      text
);

-- Загрузка десяти CSV-файлов с серверной стороны через COPY.
-- Каталог /data/ примонтирован в docker-compose.yml.
copy staging_raw from '/data/MOCK_DATA.csv'     with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (1).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (2).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (3).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (4).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (5).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (6).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (7).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (8).csv' with (format csv, header true, quote '"', escape '"');
copy staging_raw from '/data/MOCK_DATA (9).csv' with (format csv, header true, quote '"', escape '"');

-- Вспомогательный индекс для последующих join'ов на этапе ETL.
create index if not exists idx_staging_raw_email_customer on staging_raw(customer_email);
create index if not exists idx_staging_raw_email_seller   on staging_raw(seller_email);
create index if not exists idx_staging_raw_email_supplier on staging_raw(supplier_email);
