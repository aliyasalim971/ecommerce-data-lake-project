import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.functions import col, sum, round

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customers = spark.read.option("header", True).csv("s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/raw/customers/customers.csv" )
orders = spark.read.option("header", True).csv("s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/raw/orders/orders.csv")
products = spark.read.option("header", True).csv(
    "s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/raw/products/products.csv"
)
payments = spark.read.option("header", True).csv(
    "s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/raw/payments/payments.csv"
)
print("Customers")
customers.show(5)

print("Orders")
orders.show(5)

print("Products")
products.show(5)

print("Payments")
payments.show(5)

customers = customers.dropDuplicates()
orders = orders.dropDuplicates()
products = products.dropDuplicates()
payments = payments.dropDuplicates()

orders = orders.dropna(
    subset=["order_id", "customer_id", "product_id"]
)
orders = orders.withColumn("quantity", col("quantity").cast("int"))

orders = orders.withColumn(
    "unit_price",
    col("unit_price").cast("double")
)

orders = orders.withColumn(
    "total_amount",
    col("quantity") * col("unit_price")
)

sales = orders.join(
    customers,
    on="customer_id",
    how="left"
)

sales = sales.join(
    products,
    on="product_id",
    how="left"
)

sales_summary = sales.groupBy("order_date").agg(
    round(sum("total_amount"), 2).alias("daily_sales")
)

customer_revenue = sales.groupBy(
    "customer_id",
    "customer_name"
).agg(
    round(sum("total_amount"), 2).alias("customer_revenue")
)

product_performance = sales.groupBy(
    "product_id",
    "product_name",
    "category"
).agg(
    round(sum("total_amount"), 2).alias("product_sales")
)

region_sales = sales.groupBy("region").agg(
    round(sum("total_amount"), 2).alias("region_revenue")
)
sales_summary.write.mode("overwrite").parquet(
    "s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/curated/sales_summary/"
)

customer_revenue.write.mode("overwrite").parquet(
    "s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/curated/customer_revenue/"
)

product_performance.write.mode("overwrite").parquet(
    "s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/curated/product_performance/"
)

region_sales.write.mode("overwrite").parquet(
    "s3://ecommerce-sales-datalake-aliyasalim-641134884926-eu-north-1-an/curated/region_sales/"
)
job.commit()