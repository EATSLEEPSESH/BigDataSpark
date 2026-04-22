from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, quarter, monotonically_increasing_id

POSTGRES_URL = "jdbc:postgresql://postgres:5432/lab"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

spark = (
    SparkSession.builder
    .appName("ETL Raw to Star")
    .getOrCreate()
)

raw_df = (
    spark.read.jdbc(
        url=POSTGRES_URL,
        table="public.mock_data",
        properties=POSTGRES_PROPERTIES
    )
)

raw_df = raw_df.withColumn("sale_date_only", to_date(col("sale_date")))

dim_customer = (
    raw_df.select(
        col("sale_customer_id").alias("customer_id"),
        col("customer_first_name"),
        col("customer_last_name"),
        col("customer_age"),
        col("customer_email"),
        col("customer_country"),
        col("customer_postal_code"),
        col("customer_pet_type"),
        col("customer_pet_name"),
        col("customer_pet_breed")
    )
    .dropna(subset=["customer_id"])
    .dropDuplicates(["customer_id"])
)

dim_seller = (
    raw_df.select(
        col("sale_seller_id").alias("seller_id"),
        col("seller_first_name"),
        col("seller_last_name"),
        col("seller_email"),
        col("seller_country"),
        col("seller_postal_code")
    )
    .dropna(subset=["seller_id"])
    .dropDuplicates(["seller_id"])
)

dim_product = (
    raw_df.select(
        col("sale_product_id").alias("product_id"),
        col("product_name"),
        col("product_category"),
        col("product_price"),
        col("product_quantity"),
        col("pet_category"),
        col("product_weight"),
        col("product_color"),
        col("product_size"),
        col("product_brand"),
        col("product_material"),
        col("product_description"),
        col("product_rating"),
        col("product_reviews"),
        col("product_release_date"),
        col("product_expiry_date")
    )
    .dropna(subset=["product_id"])
    .dropDuplicates(["product_id"])
)

dim_store = (
    raw_df.select(
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email"
    )
    .dropDuplicates()
    .withColumn("store_id", monotonically_increasing_id() + 1)
)

dim_supplier = (
    raw_df.select(
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country"
    )
    .dropDuplicates()
    .withColumn("supplier_id", monotonically_increasing_id() + 1)
)

dim_date = (
    raw_df.select(col("sale_date_only").alias("full_date"))
    .dropna(subset=["full_date"])
    .dropDuplicates(["full_date"])
    .withColumn("date_id", monotonically_increasing_id() + 1)
    .withColumn("year", year(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
)

store_lookup = dim_store.select(
    "store_id",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email"
)

supplier_lookup = dim_supplier.select(
    "supplier_id",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country"
)

date_lookup = dim_date.select("date_id", "full_date")

fact_sales = (
    raw_df.alias("r")
    .join(
        store_lookup.alias("st"),
        on=[
            col("r.store_name") == col("st.store_name"),
            col("r.store_location") == col("st.store_location"),
            col("r.store_city") == col("st.store_city"),
            col("r.store_state") == col("st.store_state"),
            col("r.store_country") == col("st.store_country"),
            col("r.store_phone") == col("st.store_phone"),
            col("r.store_email") == col("st.store_email")
        ],
        how="left"
    )
    .join(
        supplier_lookup.alias("sp"),
        on=[
            col("r.supplier_name") == col("sp.supplier_name"),
            col("r.supplier_contact") == col("sp.supplier_contact"),
            col("r.supplier_email") == col("sp.supplier_email"),
            col("r.supplier_phone") == col("sp.supplier_phone"),
            col("r.supplier_address") == col("sp.supplier_address"),
            col("r.supplier_city") == col("sp.supplier_city"),
            col("r.supplier_country") == col("sp.supplier_country")
        ],
        how="left"
    )
    .join(
        date_lookup.alias("d"),
        col("r.sale_date_only") == col("d.full_date"),
        how="left"
    )
    .select(
        col("r.id").alias("sale_id"),
        col("r.sale_customer_id").alias("customer_id"),
        col("r.sale_seller_id").alias("seller_id"),
        col("r.sale_product_id").alias("product_id"),
        col("st.store_id").alias("store_id"),
        col("sp.supplier_id").alias("supplier_id"),
        col("d.date_id").alias("date_id"),
        col("r.sale_quantity").alias("sale_quantity"),
        col("r.sale_total_price").alias("sale_total_price"),
        col("r.product_price").alias("product_price"),
        col("r.product_rating").alias("product_rating"),
        col("r.product_reviews").alias("product_reviews")
    )
)

dim_customer.write.mode("overwrite").jdbc(
    url=POSTGRES_URL,
    table="public.dim_customer",
    properties=POSTGRES_PROPERTIES
)

dim_seller.write.mode("overwrite").jdbc(
    url=POSTGRES_URL,
    table="public.dim_seller",
    properties=POSTGRES_PROPERTIES
)

dim_product.write.mode("overwrite").jdbc(
    url=POSTGRES_URL,
    table="public.dim_product",
    properties=POSTGRES_PROPERTIES
)

dim_store.write.mode("overwrite").jdbc(
    url=POSTGRES_URL,
    table="public.dim_store",
    properties=POSTGRES_PROPERTIES
)

dim_supplier.write.mode("overwrite").jdbc(
    url=POSTGRES_URL,
    table="public.dim_supplier",
    properties=POSTGRES_PROPERTIES
)

dim_date.write.mode("overwrite").jdbc(
    url=POSTGRES_URL,
    table="public.dim_date",
    properties=POSTGRES_PROPERTIES
)

fact_sales.write.mode("overwrite").jdbc(
    url=POSTGRES_URL,
    table="public.fact_sales",
    properties=POSTGRES_PROPERTIES
)

spark.stop()