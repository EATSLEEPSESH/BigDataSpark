from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, countDistinct
from decimal import Decimal
import csv
import io
import urllib.request
import urllib.parse
import base64
import urllib.error

POSTGRES_URL = "jdbc:postgresql://postgres:5432/lab"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

CLICKHOUSE_HTTP_URL = "http://clickhouse:8123"
CLICKHOUSE_DATABASE = "lab"
CLICKHOUSE_USER = "spark_user"
CLICKHOUSE_PASSWORD = "sparkpass"

spark = (
    SparkSession.builder
    .appName("ETL Star to ClickHouse")
    .getOrCreate()
)

def ch_execute(sql):
    url = CLICKHOUSE_HTTP_URL + "/?query=" + urllib.parse.quote(sql)
    req = urllib.request.Request(url, method="POST")
    token = base64.b64encode(f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")).decode("utf-8")
    req.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(req) as resp:
        resp.read()

def py_value(v):
    if v is None:
        return r"\N"
    if isinstance(v, Decimal):
        return str(float(v))
    return str(v)

def tsv_escape(v):
    return (
        v.replace("\\", "\\\\")
         .replace("\t", "\\t")
         .replace("\n", "\\n")
         .replace("\r", "\\r")
    )

def ch_insert_df(df, table_name, columns):
    rows = df.select(*columns).collect()
    lines = []
    for row in rows:
        vals = []
        for c in columns:
            val = py_value(row[c])
            if val != r"\N":
                val = tsv_escape(val)
            vals.append(val)
        lines.append("\t".join(vals))
    data = ("\n".join(lines) + "\n").encode("utf-8")
    sql = f"INSERT INTO {CLICKHOUSE_DATABASE}.{table_name} ({', '.join(columns)}) FORMAT TabSeparated"
    url = CLICKHOUSE_HTTP_URL + "/?query=" + urllib.parse.quote(sql)
    req = urllib.request.Request(url, data=data, method="POST")
    token = base64.b64encode(f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")).decode("utf-8")
    req.add_header("Authorization", f"Basic {token}")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        print(e.read().decode("utf-8", errors="replace"))
        raise

dim_customer = spark.read.jdbc(
    url=POSTGRES_URL,
    table="public.dim_customer",
    properties=POSTGRES_PROPERTIES
)

dim_product = spark.read.jdbc(
    url=POSTGRES_URL,
    table="public.dim_product",
    properties=POSTGRES_PROPERTIES
)

dim_store = spark.read.jdbc(
    url=POSTGRES_URL,
    table="public.dim_store",
    properties=POSTGRES_PROPERTIES
)

dim_supplier = spark.read.jdbc(
    url=POSTGRES_URL,
    table="public.dim_supplier",
    properties=POSTGRES_PROPERTIES
)

dim_date = spark.read.jdbc(
    url=POSTGRES_URL,
    table="public.dim_date",
    properties=POSTGRES_PROPERTIES
)

fact_sales = spark.read.jdbc(
    url=POSTGRES_URL,
    table="public.fact_sales",
    properties=POSTGRES_PROPERTIES
)

mart_sales_products = (
    fact_sales.alias("f")
    .join(dim_product.alias("p"), col("f.product_id") == col("p.product_id"), "left")
    .groupBy(
        col("f.product_id").alias("product_id"),
        col("p.product_name").alias("product_name"),
        col("p.product_category").alias("product_category")
    )
    .agg(
        countDistinct(col("f.sale_id")).cast("long").alias("total_orders"),
        sum(col("f.sale_quantity")).cast("long").alias("total_quantity"),
        sum(col("f.sale_total_price")).cast("double").alias("total_sales"),
        avg(col("f.product_price")).cast("double").alias("avg_product_price"),
        avg(col("f.product_rating")).cast("double").alias("avg_rating"),
        avg(col("f.product_reviews")).cast("double").alias("avg_reviews")
    )
)

mart_sales_customers = (
    fact_sales.alias("f")
    .join(dim_customer.alias("c"), col("f.customer_id") == col("c.customer_id"), "left")
    .groupBy(
        col("f.customer_id").alias("customer_id"),
        col("c.customer_first_name").alias("customer_first_name"),
        col("c.customer_last_name").alias("customer_last_name"),
        col("c.customer_country").alias("customer_country"),
        col("c.customer_pet_type").alias("customer_pet_type")
    )
    .agg(
        countDistinct(col("f.sale_id")).cast("long").alias("total_orders"),
        sum(col("f.sale_quantity")).cast("long").alias("total_quantity"),
        sum(col("f.sale_total_price")).cast("double").alias("total_sales"),
        avg(col("f.sale_total_price")).cast("double").alias("avg_order_value")
    )
)

mart_sales_time = (
    fact_sales.alias("f")
    .join(dim_date.alias("d"), col("f.date_id") == col("d.date_id"), "left")
    .groupBy(
        col("d.year").cast("int").alias("year"),
        col("d.quarter").cast("int").alias("quarter"),
        col("d.month").cast("int").alias("month")
    )
    .agg(
        countDistinct(col("f.sale_id")).cast("long").alias("total_orders"),
        sum(col("f.sale_quantity")).cast("long").alias("total_quantity"),
        sum(col("f.sale_total_price")).cast("double").alias("total_sales"),
        avg(col("f.sale_total_price")).cast("double").alias("avg_order_value")
    )
)

mart_sales_stores = (
    fact_sales.alias("f")
    .join(dim_store.alias("s"), col("f.store_id") == col("s.store_id"), "left")
    .groupBy(
        col("f.store_id").alias("store_id"),
        col("s.store_name").alias("store_name"),
        col("s.store_city").alias("store_city"),
        col("s.store_state").alias("store_state"),
        col("s.store_country").alias("store_country")
    )
    .agg(
        countDistinct(col("f.sale_id")).cast("long").alias("total_orders"),
        sum(col("f.sale_quantity")).cast("long").alias("total_quantity"),
        sum(col("f.sale_total_price")).cast("double").alias("total_sales"),
        avg(col("f.sale_total_price")).cast("double").alias("avg_order_value")
    )
)

mart_sales_suppliers = (
    fact_sales.alias("f")
    .join(dim_supplier.alias("s"), col("f.supplier_id") == col("s.supplier_id"), "left")
    .groupBy(
        col("f.supplier_id").alias("supplier_id"),
        col("s.supplier_name").alias("supplier_name"),
        col("s.supplier_city").alias("supplier_city"),
        col("s.supplier_country").alias("supplier_country")
    )
    .agg(
        countDistinct(col("f.sale_id")).cast("long").alias("total_orders"),
        sum(col("f.sale_quantity")).cast("long").alias("total_quantity"),
        sum(col("f.sale_total_price")).cast("double").alias("total_sales"),
        avg(col("f.product_price")).cast("double").alias("avg_product_price")
    )
)

mart_product_quality = (
    fact_sales.alias("f")
    .join(dim_product.alias("p"), col("f.product_id") == col("p.product_id"), "left")
    .groupBy(
        col("f.product_id").alias("product_id"),
        col("p.product_name").alias("product_name"),
        col("p.product_category").alias("product_category")
    )
    .agg(
        avg(col("f.product_rating")).cast("double").alias("avg_rating"),
        avg(col("f.product_reviews")).cast("double").alias("avg_reviews"),
        sum(col("f.product_reviews")).cast("double").alias("total_reviews"),
        sum(col("f.sale_quantity")).cast("long").alias("total_quantity_sold"),
        sum(col("f.sale_total_price")).cast("double").alias("total_sales")
    )
)

ch_execute("TRUNCATE TABLE lab.mart_sales_products")
ch_execute("TRUNCATE TABLE lab.mart_sales_customers")
ch_execute("TRUNCATE TABLE lab.mart_sales_time")
ch_execute("TRUNCATE TABLE lab.mart_sales_stores")
ch_execute("TRUNCATE TABLE lab.mart_sales_suppliers")
ch_execute("TRUNCATE TABLE lab.mart_product_quality")

ch_insert_df(
    mart_sales_products,
    "mart_sales_products",
    [
        "product_id",
        "product_name",
        "product_category",
        "total_orders",
        "total_quantity",
        "total_sales",
        "avg_product_price",
        "avg_rating",
        "avg_reviews"
    ]
)

ch_insert_df(
    mart_sales_customers,
    "mart_sales_customers",
    [
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_country",
        "customer_pet_type",
        "total_orders",
        "total_quantity",
        "total_sales",
        "avg_order_value"
    ]
)

ch_insert_df(
    mart_sales_time,
    "mart_sales_time",
    [
        "year",
        "quarter",
        "month",
        "total_orders",
        "total_quantity",
        "total_sales",
        "avg_order_value"
    ]
)

ch_insert_df(
    mart_sales_stores,
    "mart_sales_stores",
    [
        "store_id",
        "store_name",
        "store_city",
        "store_state",
        "store_country",
        "total_orders",
        "total_quantity",
        "total_sales",
        "avg_order_value"
    ]
)

ch_insert_df(
    mart_sales_suppliers,
    "mart_sales_suppliers",
    [
        "supplier_id",
        "supplier_name",
        "supplier_city",
        "supplier_country",
        "total_orders",
        "total_quantity",
        "total_sales",
        "avg_product_price"
    ]
)

ch_insert_df(
    mart_product_quality,
    "mart_product_quality",
    [
        "product_id",
        "product_name",
        "product_category",
        "avg_rating",
        "avg_reviews",
        "total_reviews",
        "total_quantity_sold",
        "total_sales"
    ]
)

spark.stop()