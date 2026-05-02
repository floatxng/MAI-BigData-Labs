from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def build_star_schema(spark: SparkSession) -> None:
    raw = spark.read.jdbc(PG_URL, "staging_data", properties=PG_PROPS).cache()
    print(f"  staging_data: {raw.count()} строк")

    customer_dim = (
        raw.dropDuplicates(["customer_email"])
        .select(
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").cast("int").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_country").alias("country"),
            F.col("customer_postal_code").alias("postal_code"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
            F.col("pet_category"),
        )
        .withColumn("customer_id", F.monotonically_increasing_id())
    )
    customer_dim.write.jdbc(PG_URL, "customer_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  customer_dim: {customer_dim.count()} строк")

    seller_dim = (
        raw.dropDuplicates(["seller_email"])
        .select(
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("seller_country").alias("country"),
            F.col("seller_postal_code").alias("postal_code"),
        )
        .withColumn("seller_id", F.monotonically_increasing_id())
    )
    seller_dim.write.jdbc(PG_URL, "seller_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  seller_dim: {seller_dim.count()} строк")

    product_dim = (
        raw.dropDuplicates(["product_name", "product_category", "product_brand"])
        .select(
            F.col("product_name").alias("name"),
            F.col("product_category").alias("category"),
            F.col("product_price").cast("double").alias("price"),
            F.col("product_quantity").cast("int").alias("quantity"),
            F.col("product_weight").cast("double").alias("weight"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("description"),
            F.col("product_rating").cast("double").alias("rating"),
            F.col("product_reviews").cast("int").alias("reviews"),
            F.col("product_release_date").alias("release_date"),
            F.col("product_expiry_date").alias("expiry_date"),
        )
        .withColumn("product_id", F.monotonically_increasing_id())
    )
    product_dim.write.jdbc(PG_URL, "product_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  product_dim: {product_dim.count()} строк")

    store_dim = (
        raw.dropDuplicates(["store_name", "store_city"])
        .select(
            F.col("store_name").alias("name"),
            F.col("store_location").alias("location"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("state"),
            F.col("store_country").alias("country"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
        )
        .withColumn("store_id", F.monotonically_increasing_id())
    )
    store_dim.write.jdbc(PG_URL, "store_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  store_dim: {store_dim.count()} строк")

    supplier_dim = (
        raw.dropDuplicates(["supplier_name", "supplier_email"])
        .select(
            F.col("supplier_name").alias("name"),
            F.col("supplier_contact").alias("contact"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            F.col("supplier_country").alias("country"),
        )
        .withColumn("supplier_id", F.monotonically_increasing_id())
    )
    supplier_dim.write.jdbc(PG_URL, "supplier_dim", mode="overwrite", properties=PG_PROPS)
    print(f"  supplier_dim: {supplier_dim.count()} строк")

    customer_lookup = (
        spark.read.jdbc(PG_URL, "customer_dim", properties=PG_PROPS)
        .select(F.col("email").alias("_c_email"), "customer_id")
    )
    seller_lookup = (
        spark.read.jdbc(PG_URL, "seller_dim", properties=PG_PROPS)
        .select(F.col("email").alias("_s_email"), "seller_id")
    )
    product_lookup = (
        spark.read.jdbc(PG_URL, "product_dim", properties=PG_PROPS)
        .select(
            F.col("name").alias("_p_name"),
            F.col("category").alias("_p_cat"),
            F.col("brand").alias("_p_brand"),
            "product_id",
        )
    )
    store_lookup = (
        spark.read.jdbc(PG_URL, "store_dim", properties=PG_PROPS)
        .select(F.col("name").alias("_st_name"), F.col("city").alias("_st_city"), "store_id")
    )
    supplier_lookup = (
        spark.read.jdbc(PG_URL, "supplier_dim", properties=PG_PROPS)
        .select(
            F.col("name").alias("_su_name"),
            F.col("email").alias("_su_email"),
            "supplier_id",
        )
    )

    sales_fact = (
        raw.withColumn("sale_date_parsed", F.to_date("sale_date", "M/d/yyyy"))
        .join(
            customer_lookup,
            raw["customer_email"] == customer_lookup["_c_email"],
            "left",
        )
        .join(
            seller_lookup,
            raw["seller_email"] == seller_lookup["_s_email"],
            "left",
        )
        .join(
            product_lookup,
            (raw["product_name"] == product_lookup["_p_name"])
            & (raw["product_category"] == product_lookup["_p_cat"])
            & (raw["product_brand"] == product_lookup["_p_brand"]),
            "left",
        )
        .join(
            store_lookup,
            (raw["store_name"] == store_lookup["_st_name"])
            & (raw["store_city"] == store_lookup["_st_city"]),
            "left",
        )
        .join(
            supplier_lookup,
            (raw["supplier_name"] == supplier_lookup["_su_name"])
            & (raw["supplier_email"] == supplier_lookup["_su_email"]),
            "left",
        )
        .select(
            F.col("customer_id"),
            F.col("seller_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("supplier_id"),
            F.col("sale_date_parsed").alias("sale_date"),
            F.col("sale_quantity").cast("int").alias("quantity"),
            F.col("sale_total_price").cast("double").alias("total_price"),
        )
    )
    sales_fact.write.jdbc(PG_URL, "sales_fact", mode="overwrite", properties=PG_PROPS)
    print(f"  sales_fact: {sales_fact.count()} строк")

    raw.unpersist()


def build_reports(spark: SparkSession) -> None:
    spark.read.jdbc(PG_URL, "sales_fact", properties=PG_PROPS).createOrReplaceTempView("sf")
    spark.read.jdbc(PG_URL, "customer_dim", properties=PG_PROPS).createOrReplaceTempView("cd")
    spark.read.jdbc(PG_URL, "product_dim", properties=PG_PROPS).createOrReplaceTempView("pd")
    spark.read.jdbc(PG_URL, "store_dim", properties=PG_PROPS).createOrReplaceTempView("std")
    spark.read.jdbc(PG_URL, "supplier_dim", properties=PG_PROPS).createOrReplaceTempView("sud")

    products_top10 = spark.sql("""
        SELECT pd.name, pd.category,
               SUM(sf.quantity)    AS total_quantity,
               SUM(sf.total_price) AS total_revenue
        FROM sf JOIN pd ON sf.product_id = pd.product_id
        GROUP BY pd.name, pd.category
        ORDER BY total_quantity DESC
        LIMIT 10
    """)
    products_top10.write.jdbc(CH_URL, "report_products_top10", mode="overwrite", properties=CH_PROPS)

    products_revenue_by_category = spark.sql("""
        SELECT pd.category,
               SUM(sf.total_price) AS total_revenue
        FROM sf JOIN pd ON sf.product_id = pd.product_id
        GROUP BY pd.category
        ORDER BY total_revenue DESC
    """)
    products_revenue_by_category.write.jdbc(
        CH_URL, "report_products_revenue_by_category", mode="overwrite", properties=CH_PROPS
    )

    products_avg_rating = spark.sql("""
        SELECT pd.name, pd.category,
               AVG(pd.rating) AS avg_rating
        FROM sf JOIN pd ON sf.product_id = pd.product_id
        GROUP BY pd.name, pd.category
        ORDER BY avg_rating DESC
    """)
    products_avg_rating.write.jdbc(
        CH_URL, "report_products_avg_rating", mode="overwrite", properties=CH_PROPS
    )

    customers_top10 = spark.sql("""
        SELECT cd.first_name, cd.last_name, cd.email,
               SUM(sf.total_price) AS total_spent
        FROM sf JOIN cd ON sf.customer_id = cd.customer_id
        GROUP BY cd.first_name, cd.last_name, cd.email
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    customers_top10.write.jdbc(CH_URL, "report_customers_top10", mode="overwrite", properties=CH_PROPS)

    customers_by_country = spark.sql("""
        SELECT cd.country,
               COUNT(DISTINCT cd.customer_id) AS num_customers,
               SUM(sf.total_price)            AS total_revenue
        FROM sf JOIN cd ON sf.customer_id = cd.customer_id
        GROUP BY cd.country
        ORDER BY num_customers DESC
    """)
    customers_by_country.write.jdbc(
        CH_URL, "report_customers_by_country", mode="overwrite", properties=CH_PROPS
    )

    customers_avg_check = spark.sql("""
        SELECT cd.first_name, cd.last_name, cd.email,
               AVG(sf.total_price) AS avg_check
        FROM sf JOIN cd ON sf.customer_id = cd.customer_id
        GROUP BY cd.first_name, cd.last_name, cd.email
        ORDER BY avg_check DESC
    """)
    customers_avg_check.write.jdbc(
        CH_URL, "report_customers_avg_check", mode="overwrite", properties=CH_PROPS
    )

    time_monthly = spark.sql("""
        SELECT YEAR(sale_date)  AS year,
               MONTH(sale_date) AS month,
               SUM(total_price) AS total_revenue,
               COUNT(*)         AS num_orders
        FROM sf
        WHERE sale_date IS NOT NULL
        GROUP BY YEAR(sale_date), MONTH(sale_date)
        ORDER BY year, month
    """)
    time_monthly.write.jdbc(CH_URL, "report_time_monthly", mode="overwrite", properties=CH_PROPS)

    time_yearly = spark.sql("""
        SELECT YEAR(sale_date)  AS year,
               SUM(total_price) AS total_revenue,
               COUNT(*)         AS num_orders
        FROM sf
        WHERE sale_date IS NOT NULL
        GROUP BY YEAR(sale_date)
        ORDER BY year
    """)
    time_yearly.write.jdbc(CH_URL, "report_time_yearly", mode="overwrite", properties=CH_PROPS)

    time_avg_by_month = spark.sql("""
        SELECT YEAR(sale_date)  AS year,
               MONTH(sale_date) AS month,
               AVG(total_price) AS avg_order_size
        FROM sf
        WHERE sale_date IS NOT NULL
        GROUP BY YEAR(sale_date), MONTH(sale_date)
        ORDER BY year, month
    """)
    time_avg_by_month.write.jdbc(
        CH_URL, "report_time_avg_by_month", mode="overwrite", properties=CH_PROPS
    )

    stores_top5 = spark.sql("""
        SELECT std.name, std.city, std.country,
               SUM(sf.total_price) AS total_revenue
        FROM sf JOIN std ON sf.store_id = std.store_id
        GROUP BY std.name, std.city, std.country
        ORDER BY total_revenue DESC
        LIMIT 5
    """)
    stores_top5.write.jdbc(CH_URL, "report_stores_top5", mode="overwrite", properties=CH_PROPS)

    stores_by_location = spark.sql("""
        SELECT std.city, std.country,
               COUNT(*)            AS sales_count,
               SUM(sf.total_price) AS total_revenue
        FROM sf JOIN std ON sf.store_id = std.store_id
        GROUP BY std.city, std.country
        ORDER BY total_revenue DESC
    """)
    stores_by_location.write.jdbc(
        CH_URL, "report_stores_by_location", mode="overwrite", properties=CH_PROPS
    )

    stores_avg_check = spark.sql("""
        SELECT std.name, std.city,
               AVG(sf.total_price) AS avg_check
        FROM sf JOIN std ON sf.store_id = std.store_id
        GROUP BY std.name, std.city
        ORDER BY avg_check DESC
    """)
    stores_avg_check.write.jdbc(
        CH_URL, "report_stores_avg_check", mode="overwrite", properties=CH_PROPS
    )

    suppliers_top5 = spark.sql("""
        SELECT sud.name, sud.country,
               SUM(sf.total_price) AS total_revenue
        FROM sf JOIN sud ON sf.supplier_id = sud.supplier_id
        GROUP BY sud.name, sud.country
        ORDER BY total_revenue DESC
        LIMIT 5
    """)
    suppliers_top5.write.jdbc(CH_URL, "report_suppliers_top5", mode="overwrite", properties=CH_PROPS)

    suppliers_avg_price = spark.sql("""
        SELECT sud.name,
               AVG(pd.price) AS avg_product_price
        FROM sf
        JOIN sud ON sf.supplier_id = sud.supplier_id
        JOIN pd  ON sf.product_id  = pd.product_id
        GROUP BY sud.name
        ORDER BY avg_product_price DESC
    """)
    suppliers_avg_price.write.jdbc(
        CH_URL, "report_suppliers_avg_price", mode="overwrite", properties=CH_PROPS
    )

    suppliers_by_country = spark.sql("""
        SELECT sud.country,
               SUM(sf.total_price)            AS total_revenue,
               COUNT(DISTINCT sud.supplier_id) AS num_suppliers
        FROM sf JOIN sud ON sf.supplier_id = sud.supplier_id
        GROUP BY sud.country
        ORDER BY total_revenue DESC
    """)
    suppliers_by_country.write.jdbc(
        CH_URL, "report_suppliers_by_country", mode="overwrite", properties=CH_PROPS
    )

    quality_top_rated = spark.sql("""
        SELECT pd.name, pd.category, pd.rating
        FROM pd
        WHERE pd.rating IS NOT NULL
        ORDER BY pd.rating DESC
        LIMIT 10
    """)
    quality_top_rated.write.jdbc(CH_URL, "report_quality_top_rated", mode="overwrite", properties=CH_PROPS)

    quality_rating_vs_sales = spark.sql("""
        SELECT pd.name, pd.category,
               pd.rating,
               SUM(sf.quantity)    AS total_quantity_sold,
               SUM(sf.total_price) AS total_revenue
        FROM sf JOIN pd ON sf.product_id = pd.product_id
        GROUP BY pd.name, pd.category, pd.rating
        ORDER BY pd.rating DESC
    """)
    quality_rating_vs_sales.write.jdbc(
        CH_URL, "report_quality_rating_vs_sales", mode="overwrite", properties=CH_PROPS
    )

    quality_most_reviewed = spark.sql("""
        SELECT pd.name, pd.category, pd.reviews
        FROM pd
        WHERE pd.reviews IS NOT NULL
        ORDER BY pd.reviews DESC
        LIMIT 10
    """)
    quality_most_reviewed.write.jdbc(
        CH_URL, "report_quality_most_reviewed", mode="overwrite", properties=CH_PROPS
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("MAI BigData Lab 2 Pipeline")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    build_star_schema(spark)
    build_reports(spark)
    spark.stop()


if __name__ == "__main__":
    main()
