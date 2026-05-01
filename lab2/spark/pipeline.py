"""
=====================================================================
MAI BigData Lab 2 - единый ETL-пайплайн на Spark SQL.

Этап 1 (build_star_schema): сырые данные из staging_data в PostgreSQL
                            трансформируются в звёздную схему
                            (6 dim-таблиц + sales_fact).
Этап 2 (build_reports):     по звёздной схеме строится 6 аналитических
                            витрин и записываются в ClickHouse.

Стиль реализации - Spark SQL с временными представлениями
(createOrReplaceTempView + spark.sql), что отличается от подхода
с цепочками методов DataFrame API.
=====================================================================
"""
from pyspark.sql import SparkSession


# --- Параметры подключения ----------------------------------------
PG_URL = "jdbc:postgresql://postgres:5432/mai_lab2_db"
PG_PROPS = {
    "user": "student",
    "password": "bigdata2026",
    "driver": "org.postgresql.Driver",
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_PROPS = {
    "user": "click",
    "password": "click",
    "driver": "cc.blynk.clickhouse.ClickHouseDriver",
}


# =====================================================================
# Этап 1. Построение звёздной схемы в PostgreSQL.
# =====================================================================
def build_star_schema(spark: SparkSession) -> None:
    """Из staging_data строим 6 dim-таблиц и sales_fact (через Spark SQL)."""

    # --- Читаем staging-таблицу как временное представление ----------
    print("Читаем staging_data из PostgreSQL...")
    raw = spark.read.jdbc(PG_URL, "staging_data", properties=PG_PROPS)
    raw = raw.cache()
    print(f"  всего сырых строк: {raw.count()}")
    raw.createOrReplaceTempView("staging")

    # -----------------------------------------------------------------
    # customer_dim - дедупликация по customer_email
    # -----------------------------------------------------------------
    print("Строим customer_dim...")
    customer_dim = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY customer_email) AS customer_id,
            customer_first_name AS first_name,
            customer_last_name  AS last_name,
            CAST(customer_age AS INT) AS age,
            customer_email      AS email,
            customer_country    AS country,
            customer_postal_code AS postal_code,
            customer_pet_type   AS pet_type,
            customer_pet_name   AS pet_name,
            customer_pet_breed  AS pet_breed,
            pet_category
        FROM (
            SELECT
                staging.*,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_email
                    ORDER BY customer_email
                ) AS rn
            FROM staging
        ) t
        WHERE rn = 1
    """)
    customer_dim.write.jdbc(PG_URL, "customer_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  customer_dim: {customer_dim.count()} строк")

    # -----------------------------------------------------------------
    # seller_dim - дедупликация по seller_email
    # -----------------------------------------------------------------
    print("Строим seller_dim...")
    seller_dim = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY seller_email) AS seller_id,
            seller_first_name   AS first_name,
            seller_last_name    AS last_name,
            seller_email        AS email,
            seller_country      AS country,
            seller_postal_code  AS postal_code
        FROM (
            SELECT
                staging.*,
                ROW_NUMBER() OVER (
                    PARTITION BY seller_email
                    ORDER BY seller_email
                ) AS rn
            FROM staging
        ) t
        WHERE rn = 1
    """)
    seller_dim.write.jdbc(PG_URL, "seller_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  seller_dim: {seller_dim.count()} строк")

    # -----------------------------------------------------------------
    # product_dim - дедупликация по (product_name, product_category, product_brand)
    # -----------------------------------------------------------------
    print("Строим product_dim...")
    product_dim = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY product_name, product_category, product_brand) AS product_id,
            product_name          AS name,
            product_category      AS category,
            CAST(product_price    AS DOUBLE) AS price,
            CAST(product_quantity AS INT)    AS quantity,
            CAST(product_weight   AS DOUBLE) AS weight,
            product_color         AS color,
            product_size          AS size,
            product_brand         AS brand,
            product_material      AS material,
            product_description   AS description,
            CAST(product_rating   AS DOUBLE) AS rating,
            CAST(product_reviews  AS INT)    AS reviews,
            product_release_date  AS release_date,
            product_expiry_date   AS expiry_date
        FROM (
            SELECT
                staging.*,
                ROW_NUMBER() OVER (
                    PARTITION BY product_name, product_category, product_brand
                    ORDER BY product_name
                ) AS rn
            FROM staging
        ) t
        WHERE rn = 1
    """)
    product_dim.write.jdbc(PG_URL, "product_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  product_dim: {product_dim.count()} строк")

    # -----------------------------------------------------------------
    # store_dim - дедупликация по (store_name, store_city)
    # -----------------------------------------------------------------
    print("Строим store_dim...")
    store_dim = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY store_name, store_city) AS store_id,
            store_name      AS name,
            store_location  AS location,
            store_city      AS city,
            store_state     AS state,
            store_country   AS country,
            store_phone     AS phone,
            store_email     AS email
        FROM (
            SELECT
                staging.*,
                ROW_NUMBER() OVER (
                    PARTITION BY store_name, store_city
                    ORDER BY store_name
                ) AS rn
            FROM staging
        ) t
        WHERE rn = 1
    """)
    store_dim.write.jdbc(PG_URL, "store_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  store_dim: {store_dim.count()} строк")

    # -----------------------------------------------------------------
    # supplier_dim - дедупликация по (supplier_name, supplier_email)
    # -----------------------------------------------------------------
    print("Строим supplier_dim...")
    supplier_dim = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY supplier_name, supplier_email) AS supplier_id,
            supplier_name     AS name,
            supplier_contact  AS contact,
            supplier_email    AS email,
            supplier_phone    AS phone,
            supplier_address  AS address,
            supplier_city     AS city,
            supplier_country  AS country
        FROM (
            SELECT
                staging.*,
                ROW_NUMBER() OVER (
                    PARTITION BY supplier_name, supplier_email
                    ORDER BY supplier_name
                ) AS rn
            FROM staging
        ) t
        WHERE rn = 1
    """)
    supplier_dim.write.jdbc(PG_URL, "supplier_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  supplier_dim: {supplier_dim.count()} строк")

    # -----------------------------------------------------------------
    # calendar_dim - уникальные даты продаж (sale_date в формате M/d/yyyy)
    # -----------------------------------------------------------------
    print("Строим calendar_dim...")
    calendar_dim = spark.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY full_date) AS calendar_id,
            full_date,
            DAYOFMONTH(full_date) AS day,
            MONTH(full_date)      AS month,
            YEAR(full_date)       AS year,
            QUARTER(full_date)    AS quarter
        FROM (
            SELECT DISTINCT TO_DATE(sale_date, 'M/d/yyyy') AS full_date
            FROM staging
            WHERE sale_date IS NOT NULL
        ) d
    """)
    calendar_dim.write.jdbc(PG_URL, "calendar_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  calendar_dim: {calendar_dim.count()} строк")

    # -----------------------------------------------------------------
    # sales_fact - перечитываем dim-таблицы из БД, чтобы получить
    # уже сохранённые ID, регистрируем как views и собираем факт через JOIN.
    # -----------------------------------------------------------------
    print("Строим sales_fact...")
    spark.read.jdbc(PG_URL, "customer_dim", properties=PG_PROPS).createOrReplaceTempView("v_customer")
    spark.read.jdbc(PG_URL, "seller_dim",   properties=PG_PROPS).createOrReplaceTempView("v_seller")
    spark.read.jdbc(PG_URL, "product_dim",  properties=PG_PROPS).createOrReplaceTempView("v_product")
    spark.read.jdbc(PG_URL, "store_dim",    properties=PG_PROPS).createOrReplaceTempView("v_store")
    spark.read.jdbc(PG_URL, "supplier_dim", properties=PG_PROPS).createOrReplaceTempView("v_supplier")
    spark.read.jdbc(PG_URL, "calendar_dim", properties=PG_PROPS).createOrReplaceTempView("v_calendar")

    sales_fact = spark.sql("""
        SELECT
            c.customer_id,
            se.seller_id,
            p.product_id,
            st.store_id,
            sup.supplier_id,
            cal.calendar_id,
            CAST(s.sale_quantity    AS INT)    AS quantity,
            CAST(s.sale_total_price AS DOUBLE) AS total_price
        FROM staging s
        LEFT JOIN v_customer c
               ON s.customer_email = c.email
        LEFT JOIN v_seller se
               ON s.seller_email = se.email
        LEFT JOIN v_product p
               ON s.product_name     = p.name
              AND s.product_category = p.category
              AND s.product_brand    = p.brand
        LEFT JOIN v_store st
               ON s.store_name = st.name
              AND s.store_city = st.city
        LEFT JOIN v_supplier sup
               ON s.supplier_name  = sup.name
              AND s.supplier_email = sup.email
        LEFT JOIN v_calendar cal
               ON TO_DATE(s.sale_date, 'M/d/yyyy') = cal.full_date
    """)
    sales_fact.write.jdbc(PG_URL, "sales_fact", mode="overwrite", properties=PG_PROPS)
    print(f"  sales_fact: {sales_fact.count()} строк")

    raw.unpersist()
    print("Звёздная схема построена.")


# =====================================================================
# Этап 2. Аналитические витрины в ClickHouse.
# =====================================================================
def build_reports(spark: SparkSession) -> None:
    """Из звёздной схемы строим 6 отчётов и пишем их в ClickHouse."""

    print("Читаем звёздную схему из PostgreSQL...")
    spark.read.jdbc(PG_URL, "sales_fact",   properties=PG_PROPS).createOrReplaceTempView("sf")
    spark.read.jdbc(PG_URL, "customer_dim", properties=PG_PROPS).createOrReplaceTempView("cd")
    spark.read.jdbc(PG_URL, "seller_dim",   properties=PG_PROPS).createOrReplaceTempView("sd")
    spark.read.jdbc(PG_URL, "product_dim",  properties=PG_PROPS).createOrReplaceTempView("pd")
    spark.read.jdbc(PG_URL, "store_dim",    properties=PG_PROPS).createOrReplaceTempView("std")
    spark.read.jdbc(PG_URL, "supplier_dim", properties=PG_PROPS).createOrReplaceTempView("sud")
    spark.read.jdbc(PG_URL, "calendar_dim", properties=PG_PROPS).createOrReplaceTempView("cald")

    # -----------------------------------------------------------------
    # 1. report_products - продажи по продуктам, отсортированы по выручке.
    # -----------------------------------------------------------------
    print("Строим report_products...")
    report_products = spark.sql("""
        SELECT
            pd.name     AS product_name,
            pd.category AS product_category,
            SUM(sf.quantity)    AS sum_quantity,
            SUM(sf.total_price) AS sum_revenue,
            AVG(pd.rating)      AS avg_rating,
            SUM(pd.reviews)     AS sum_reviews
        FROM sf
        JOIN pd ON sf.product_id = pd.product_id
        GROUP BY pd.name, pd.category
        ORDER BY sum_revenue DESC
    """)
    report_products.write.jdbc(CH_URL, "report_products", mode="overwrite", properties=CH_PROPS)
    print(f"  report_products: {report_products.count()} строк")

    # -----------------------------------------------------------------
    # 2. report_customers - сводка по клиентам.
    #    Имя в формате "Фамилия, Имя" (через ", ").
    # -----------------------------------------------------------------
    print("Строим report_customers...")
    report_customers = spark.sql("""
        SELECT
            CONCAT_WS(', ', cd.last_name, cd.first_name) AS full_name,
            cd.country,
            SUM(sf.total_price) AS total_amount,
            COUNT(*)            AS num_orders,
            AVG(sf.total_price) AS avg_check,
            MAX(sf.total_price) AS max_purchase
        FROM sf
        JOIN cd ON sf.customer_id = cd.customer_id
        GROUP BY CONCAT_WS(', ', cd.last_name, cd.first_name), cd.country
        ORDER BY total_amount DESC
    """)
    report_customers.write.jdbc(CH_URL, "report_customers", mode="overwrite", properties=CH_PROPS)
    print(f"  report_customers: {report_customers.count()} строк")

    # -----------------------------------------------------------------
    # 3. report_timeline - агрегация по (year, month, quarter).
    # -----------------------------------------------------------------
    print("Строим report_timeline...")
    report_timeline = spark.sql("""
        SELECT
            cald.year,
            cald.month,
            cald.quarter,
            SUM(sf.total_price) AS total_amount,
            COUNT(*)            AS orders_count,
            AVG(sf.total_price) AS avg_order,
            MIN(sf.total_price) AS min_order,
            MAX(sf.total_price) AS max_order
        FROM sf
        JOIN cald ON sf.calendar_id = cald.calendar_id
        GROUP BY cald.year, cald.month, cald.quarter
        ORDER BY cald.year, cald.month
    """)
    report_timeline.write.jdbc(CH_URL, "report_timeline", mode="overwrite", properties=CH_PROPS)
    print(f"  report_timeline: {report_timeline.count()} строк")

    # -----------------------------------------------------------------
    # 4. report_stores - топ-5 магазинов по выручке.
    # -----------------------------------------------------------------
    print("Строим report_stores...")
    report_stores = spark.sql("""
        SELECT
            std.name    AS store_name,
            std.city    AS store_city,
            std.country AS store_country,
            SUM(sf.total_price) AS revenue,
            COUNT(*)            AS sales_count,
            AVG(sf.total_price) AS avg_check
        FROM sf
        JOIN std ON sf.store_id = std.store_id
        GROUP BY std.name, std.city, std.country
        ORDER BY revenue DESC
        LIMIT 5
    """)
    report_stores.write.jdbc(CH_URL, "report_stores", mode="overwrite", properties=CH_PROPS)
    print(f"  report_stores: {report_stores.count()} строк")

    # -----------------------------------------------------------------
    # 5. report_suppliers - сводка по поставщикам.
    #    Дополнительно distinct_products_count - сколько разных
    #    товаров поставщик реально продал.
    # -----------------------------------------------------------------
    print("Строим report_suppliers...")
    report_suppliers = spark.sql("""
        SELECT
            sud.name    AS supplier_name,
            sud.country AS supplier_country,
            SUM(sf.total_price)        AS revenue,
            AVG(pd.price)              AS avg_product_price,
            COUNT(*)                   AS products_sold,
            COUNT(DISTINCT pd.product_id) AS distinct_products_count
        FROM sf
        JOIN sud ON sf.supplier_id = sud.supplier_id
        JOIN pd  ON sf.product_id  = pd.product_id
        GROUP BY sud.name, sud.country
        ORDER BY revenue DESC
    """)
    report_suppliers.write.jdbc(CH_URL, "report_suppliers", mode="overwrite", properties=CH_PROPS)
    print(f"  report_suppliers: {report_suppliers.count()} строк")

    # -----------------------------------------------------------------
    # 6. report_quality - качество продуктов.
    #    sales_per_review - отношение проданных штук к числу отзывов
    #    (защита от деления на ноль через NULLIF).
    # -----------------------------------------------------------------
    print("Строим report_quality...")
    report_quality = spark.sql("""
        SELECT
            pd.name     AS product_name,
            pd.category AS product_category,
            AVG(pd.rating)      AS avg_rating,
            SUM(pd.reviews)     AS sum_reviews,
            SUM(sf.quantity)    AS total_quantity,
            SUM(sf.total_price) AS total_revenue,
            SUM(sf.quantity) / NULLIF(SUM(pd.reviews), 0) AS sales_per_review
        FROM sf
        JOIN pd ON sf.product_id = pd.product_id
        GROUP BY pd.name, pd.category
        ORDER BY avg_rating DESC
    """)
    report_quality.write.jdbc(CH_URL, "report_quality", mode="overwrite", properties=CH_PROPS)
    print(f"  report_quality: {report_quality.count()} строк")

    print("Все 6 витрин записаны в ClickHouse.")


# =====================================================================
# Точка входа.
# =====================================================================
def main() -> None:
    spark = (
        SparkSession.builder
        .appName("MAI BigData Lab 2 Pipeline")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    print("=== Этап 1: Построение звёздной схемы в PostgreSQL ===")
    build_star_schema(spark)

    print("=== Этап 2: Построение отчётов в ClickHouse ===")
    build_reports(spark)

    print("=== Pipeline complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
