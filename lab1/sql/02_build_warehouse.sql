-- =====================================================================
-- 02_build_warehouse.sql
-- Полное построение «снежинки»: DDL + DML в одном файле.
-- Порядок: сброс таблиц -> поддименции -> измерения -> факт -> индексы.
-- Дедупликация:
--   * клиенты   — по customer_email
--   * продавцы  — по seller_email
--   * товары    — по (product_name, product_category, product_brand)
--   * магазины  — по (store_name, store_city, store_country)
--   * поставщики— по supplier_email
-- ID из CSV игнорируются (повторяются между файлами).
-- =====================================================================

-- Сбрасываем все витрины, staging оставляем как есть.
drop table if exists fact_sales              cascade;
drop table if exists dim_customer            cascade;
drop table if exists dim_seller              cascade;
drop table if exists dim_product             cascade;
drop table if exists dim_store               cascade;
drop table if exists dim_supplier            cascade;
drop table if exists dim_calendar            cascade;
drop table if exists dim_address_customer    cascade;
drop table if exists dim_pet_info            cascade;
drop table if exists dim_address_seller      cascade;
drop table if exists dim_address_store       cascade;
drop table if exists dim_address_supplier    cascade;
drop table if exists dim_category_product    cascade;
drop table if exists dim_attributes_product  cascade;

-- =====================================================================
-- ПОДДИМЕНЦИИ (второй уровень)
-- =====================================================================

-- Адрес клиента
create table dim_address_customer (
    address_id   serial primary key,
    country      varchar(200),
    postal_code  varchar(50)
);

-- Информация о питомце клиента
create table dim_pet_info (
    pet_id    serial primary key,
    type      varchar(100),
    name      varchar(200),
    breed     varchar(200),
    category  varchar(100)
);

-- Адрес продавца
create table dim_address_seller (
    address_id   serial primary key,
    country      varchar(200),
    postal_code  varchar(50)
);

-- Адрес магазина
create table dim_address_store (
    address_id  serial primary key,
    city        varchar(200),
    state       varchar(200),
    country     varchar(200)
);

-- Адрес поставщика
create table dim_address_supplier (
    address_id    serial primary key,
    address_line  varchar(300),
    city          varchar(200),
    country       varchar(200)
);

-- Категория товара
create table dim_category_product (
    category_id  serial primary key,
    name         varchar(200)
);

-- Атрибуты товара (характеристики)
create table dim_attributes_product (
    attribute_id   serial primary key,
    weight         numeric(10,2),
    color          varchar(100),
    size           varchar(50),
    material       varchar(200),
    description    text,
    rating         numeric(3,1),
    reviews_count  int,
    released_on    date,
    expires_on     date
);

-- =====================================================================
-- ЗАПОЛНЕНИЕ ПОДДИМЕНЦИЙ
-- =====================================================================

-- Адрес клиента: дедуп по (country, postal_code)
insert into dim_address_customer (country, postal_code)
select distinct
    coalesce(customer_country, ''),
    coalesce(customer_postal_code, '')
from staging_raw;

-- Питомец: дедуп по полному набору признаков
insert into dim_pet_info (type, name, breed, category)
select distinct
    coalesce(customer_pet_type, ''),
    coalesce(customer_pet_name, ''),
    coalesce(customer_pet_breed, ''),
    coalesce(pet_category, '')
from staging_raw;

-- Адрес продавца
insert into dim_address_seller (country, postal_code)
select distinct
    coalesce(seller_country, ''),
    coalesce(seller_postal_code, '')
from staging_raw;

-- Адрес магазина
insert into dim_address_store (city, state, country)
select distinct
    coalesce(store_city, ''),
    coalesce(store_state, ''),
    coalesce(store_country, '')
from staging_raw;

-- Адрес поставщика
insert into dim_address_supplier (address_line, city, country)
select distinct
    coalesce(supplier_address, ''),
    coalesce(supplier_city, ''),
    coalesce(supplier_country, '')
from staging_raw;

-- Категория товара
insert into dim_category_product (name)
select distinct
    coalesce(product_category, '')
from staging_raw;

-- Атрибуты товара — каждая комбинация характеристик считается уникальной
insert into dim_attributes_product (weight, color, size, material, description, rating, reviews_count, released_on, expires_on)
select distinct
    case when product_weight ~ '^\d+(\.\d+)?$' then product_weight::numeric else null end,
    coalesce(product_color, ''),
    coalesce(product_size, ''),
    coalesce(product_material, ''),
    coalesce(product_description, ''),
    case when product_rating ~ '^\d+(\.\d+)?$' then product_rating::numeric else null end,
    case when product_reviews ~ '^\d+$' then product_reviews::int else null end,
    case when product_release_date <> '' and product_release_date is not null
         then to_date(product_release_date, 'MM/DD/YYYY') else null end,
    case when product_expiry_date <> '' and product_expiry_date is not null
         then to_date(product_expiry_date, 'MM/DD/YYYY') else null end
from staging_raw;

-- =====================================================================
-- ИЗМЕРЕНИЯ (первый уровень) — DDL
-- =====================================================================

-- Клиент
create table dim_customer (
    id           serial primary key,
    first_name   varchar(200),
    last_name    varchar(200),
    age          int,
    email        varchar(300),
    address_id   int references dim_address_customer(address_id),
    pet_id       int references dim_pet_info(pet_id)
);

-- Продавец
create table dim_seller (
    id          serial primary key,
    first_name  varchar(200),
    last_name   varchar(200),
    email       varchar(300),
    address_id  int references dim_address_seller(address_id)
);

-- Товар
create table dim_product (
    id            serial primary key,
    name          varchar(300),
    price         numeric(10,2),
    quantity      int,
    brand         varchar(200),
    category_id   int references dim_category_product(category_id),
    attribute_id  int references dim_attributes_product(attribute_id)
);

-- Магазин
create table dim_store (
    id               serial primary key,
    name             varchar(300),
    location_detail  varchar(300),
    phone            varchar(50),
    email            varchar(300),
    address_id       int references dim_address_store(address_id)
);

-- Поставщик
create table dim_supplier (
    id          serial primary key,
    name        varchar(300),
    contact     varchar(300),
    email       varchar(300),
    phone       varchar(50),
    address_id  int references dim_address_supplier(address_id)
);

-- Календарь
create table dim_calendar (
    id            serial primary key,
    full_date     date unique,
    day_num       int,
    month_num     int,
    year_num      int,
    quarter_num   int
);

-- =====================================================================
-- ЗАПОЛНЕНИЕ ИЗМЕРЕНИЙ
-- =====================================================================

-- Клиенты: дедуп по customer_email (email — уникальный ключ)
insert into dim_customer (first_name, last_name, age, email, address_id, pet_id)
select
    c.customer_first_name,
    c.customer_last_name,
    case when c.customer_age ~ '^\d+$' then c.customer_age::int else null end,
    c.customer_email,
    ac.address_id,
    pi.pet_id
from (
    select distinct on (customer_email)
        customer_first_name,
        customer_last_name,
        customer_age,
        customer_email,
        coalesce(customer_country, '')     as customer_country,
        coalesce(customer_postal_code, '') as customer_postal_code,
        coalesce(customer_pet_type, '')    as customer_pet_type,
        coalesce(customer_pet_name, '')    as customer_pet_name,
        coalesce(customer_pet_breed, '')   as customer_pet_breed,
        coalesce(pet_category, '')         as pet_category
    from staging_raw
    where customer_email is not null
    order by customer_email
) c
join dim_address_customer ac
  on ac.country     = c.customer_country
 and ac.postal_code = c.customer_postal_code
join dim_pet_info pi
  on pi.type     = c.customer_pet_type
 and pi.name     = c.customer_pet_name
 and pi.breed    = c.customer_pet_breed
 and pi.category = c.pet_category;

-- Продавцы: дедуп по seller_email
insert into dim_seller (first_name, last_name, email, address_id)
select
    s.seller_first_name,
    s.seller_last_name,
    s.seller_email,
    asl.address_id
from (
    select distinct on (seller_email)
        seller_first_name,
        seller_last_name,
        seller_email,
        coalesce(seller_country, '')     as seller_country,
        coalesce(seller_postal_code, '') as seller_postal_code
    from staging_raw
    where seller_email is not null
    order by seller_email
) s
join dim_address_seller asl
  on asl.country     = s.seller_country
 and asl.postal_code = s.seller_postal_code;

-- Товары: дедуп по (product_name, product_category, product_brand)
insert into dim_product (name, price, quantity, brand, category_id, attribute_id)
select
    p.product_name,
    case when p.product_price ~ '^\d+(\.\d+)?$' then p.product_price::numeric else null end,
    case when p.product_quantity ~ '^\d+$' then p.product_quantity::int else null end,
    p.product_brand,
    cp.category_id,
    ap.attribute_id
from (
    select distinct on (product_name, coalesce(product_category, ''), coalesce(product_brand, ''))
        product_name,
        coalesce(product_category, '') as product_category,
        coalesce(product_brand, '')    as product_brand,
        product_price,
        product_quantity,
        case when product_weight ~ '^\d+(\.\d+)?$' then product_weight::numeric else null end as pw,
        coalesce(product_color, '')     as pcolor,
        coalesce(product_size, '')      as psize,
        coalesce(product_material, '')  as pmat,
        coalesce(product_description, '') as pdesc,
        case when product_rating ~ '^\d+(\.\d+)?$' then product_rating::numeric else null end as prat,
        case when product_reviews ~ '^\d+$' then product_reviews::int else null end as prev,
        case when product_release_date <> '' and product_release_date is not null
             then to_date(product_release_date, 'MM/DD/YYYY') else null end as prel,
        case when product_expiry_date <> '' and product_expiry_date is not null
             then to_date(product_expiry_date, 'MM/DD/YYYY') else null end as pexp
    from staging_raw
    order by product_name, coalesce(product_category, ''), coalesce(product_brand, '')
) p
join dim_category_product cp
  on cp.name = p.product_category
join dim_attributes_product ap
  on ap.color       = p.pcolor
 and ap.size        = p.psize
 and ap.material    = p.pmat
 and ap.description = p.pdesc
 and (ap.weight        is not distinct from p.pw)
 and (ap.rating        is not distinct from p.prat)
 and (ap.reviews_count is not distinct from p.prev)
 and (ap.released_on   is not distinct from p.prel)
 and (ap.expires_on    is not distinct from p.pexp);

-- Магазины: дедуп по (store_name, store_city, store_country)
insert into dim_store (name, location_detail, phone, email, address_id)
select
    st.store_name,
    st.store_location,
    st.store_phone,
    st.store_email,
    ads.address_id
from (
    select distinct on (store_name, coalesce(store_city, ''), coalesce(store_country, ''))
        store_name,
        coalesce(store_location, '') as store_location,
        store_phone,
        store_email,
        coalesce(store_city, '')     as store_city,
        coalesce(store_state, '')    as store_state,
        coalesce(store_country, '')  as store_country
    from staging_raw
    order by store_name, coalesce(store_city, ''), coalesce(store_country, '')
) st
join dim_address_store ads
  on ads.city    = st.store_city
 and ads.state   = st.store_state
 and ads.country = st.store_country;

-- Поставщики: дедуп по supplier_email
insert into dim_supplier (name, contact, email, phone, address_id)
select
    su.supplier_name,
    su.supplier_contact,
    su.supplier_email,
    su.supplier_phone,
    asu.address_id
from (
    select distinct on (supplier_email)
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        coalesce(supplier_address, '') as supplier_address,
        coalesce(supplier_city, '')    as supplier_city,
        coalesce(supplier_country, '') as supplier_country
    from staging_raw
    where supplier_email is not null
    order by supplier_email
) su
join dim_address_supplier asu
  on asu.address_line = su.supplier_address
 and asu.city         = su.supplier_city
 and asu.country      = su.supplier_country;

-- Календарь: уникальные даты продаж
insert into dim_calendar (full_date, day_num, month_num, year_num, quarter_num)
select distinct
    to_date(sale_date, 'MM/DD/YYYY'),
    extract(day     from to_date(sale_date, 'MM/DD/YYYY'))::int,
    extract(month   from to_date(sale_date, 'MM/DD/YYYY'))::int,
    extract(year    from to_date(sale_date, 'MM/DD/YYYY'))::int,
    extract(quarter from to_date(sale_date, 'MM/DD/YYYY'))::int
from staging_raw
where sale_date is not null and sale_date <> '';

-- =====================================================================
-- ФАКТОВАЯ ТАБЛИЦА
-- =====================================================================

create table fact_sales (
    id            serial primary key,
    quantity      int,
    total_price   numeric(12,2),
    customer_id   int references dim_customer(id),
    seller_id     int references dim_seller(id),
    product_id    int references dim_product(id),
    store_id      int references dim_store(id),
    supplier_id   int references dim_supplier(id),
    calendar_id   int references dim_calendar(id)
);

-- Заполнение факта: по строке staging — одна строка факта.
-- Соединяемся с измерениями по естественным ключам (email, имя+категория+бренд и т.п.).
insert into fact_sales (quantity, total_price, customer_id, seller_id, product_id, store_id, supplier_id, calendar_id)
select
    case when r.sale_quantity ~ '^\d+$' then r.sale_quantity::int else null end,
    case when r.sale_total_price ~ '^\d+(\.\d+)?$' then r.sale_total_price::numeric else null end,
    dc.id,
    ds.id,
    dp.id,
    dst.id,
    dsu.id,
    dcal.id
from staging_raw r
join dim_customer dc
  on dc.email = r.customer_email
join dim_seller ds
  on ds.email = r.seller_email
join dim_product dp
  on dp.name  = r.product_name
 and dp.brand = coalesce(r.product_brand, '')
 and dp.category_id = (
     select cp.category_id from dim_category_product cp
     where cp.name = coalesce(r.product_category, '')
 )
join dim_store dst
  on dst.name = r.store_name
 and dst.address_id = (
     select ads.address_id from dim_address_store ads
     where ads.city    = coalesce(r.store_city, '')
       and ads.state   = coalesce(r.store_state, '')
       and ads.country = coalesce(r.store_country, '')
 )
join dim_supplier dsu
  on dsu.email = r.supplier_email
join dim_calendar dcal
  on dcal.full_date = to_date(r.sale_date, 'MM/DD/YYYY');

-- =====================================================================
-- ИНДЕКСЫ
-- =====================================================================

-- Внешние ключи факта
create index idx_fact_sales_customer  on fact_sales(customer_id);
create index idx_fact_sales_seller    on fact_sales(seller_id);
create index idx_fact_sales_product   on fact_sales(product_id);
create index idx_fact_sales_store     on fact_sales(store_id);
create index idx_fact_sales_supplier  on fact_sales(supplier_id);
create index idx_fact_sales_calendar  on fact_sales(calendar_id);

-- Внешние ключи измерений
create index idx_dim_customer_address  on dim_customer(address_id);
create index idx_dim_customer_pet      on dim_customer(pet_id);
create index idx_dim_seller_address    on dim_seller(address_id);
create index idx_dim_product_category  on dim_product(category_id);
create index idx_dim_product_attribute on dim_product(attribute_id);
create index idx_dim_store_address     on dim_store(address_id);
create index idx_dim_supplier_address  on dim_supplier(address_id);

-- Естественные ключи (используются в join'ах при заполнении факта)
create index idx_dim_customer_email      on dim_customer(email);
create index idx_dim_seller_email        on dim_seller(email);
create index idx_dim_supplier_email      on dim_supplier(email);
create index idx_dim_product_lookup      on dim_product(name, brand, category_id);
create index idx_dim_store_lookup        on dim_store(name, address_id);
create index idx_dim_calendar_full_date  on dim_calendar(full_date);

-- =====================================================================
-- ПРОВЕРКА: вывод количества строк во всех таблицах
-- =====================================================================

do $$
declare
    cnt bigint;
begin
    select count(*) into cnt from staging_raw;
    raise notice 'staging_raw                : %', cnt;
    select count(*) into cnt from dim_address_customer;
    raise notice 'dim_address_customer       : %', cnt;
    select count(*) into cnt from dim_pet_info;
    raise notice 'dim_pet_info               : %', cnt;
    select count(*) into cnt from dim_address_seller;
    raise notice 'dim_address_seller         : %', cnt;
    select count(*) into cnt from dim_address_store;
    raise notice 'dim_address_store          : %', cnt;
    select count(*) into cnt from dim_address_supplier;
    raise notice 'dim_address_supplier       : %', cnt;
    select count(*) into cnt from dim_category_product;
    raise notice 'dim_category_product       : %', cnt;
    select count(*) into cnt from dim_attributes_product;
    raise notice 'dim_attributes_product     : %', cnt;
    select count(*) into cnt from dim_customer;
    raise notice 'dim_customer               : %', cnt;
    select count(*) into cnt from dim_seller;
    raise notice 'dim_seller                 : %', cnt;
    select count(*) into cnt from dim_product;
    raise notice 'dim_product                : %', cnt;
    select count(*) into cnt from dim_store;
    raise notice 'dim_store                  : %', cnt;
    select count(*) into cnt from dim_supplier;
    raise notice 'dim_supplier               : %', cnt;
    select count(*) into cnt from dim_calendar;
    raise notice 'dim_calendar               : %', cnt;
    select count(*) into cnt from fact_sales;
    raise notice 'fact_sales                 : %', cnt;
end $$;
