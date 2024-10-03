from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Order Period Analysis").getOrCreate()

# Define the file paths
orders_path = "gs://dataproc-staging-us-central1-198060065126-ydwngrbp/week3-pyspark/input/orders.csv"
order_products_path = "gs://dataproc-staging-us-central1-198060065126-ydwngrbp/week3-pyspark/input/order_products.csv"
cleaned_products_path = "gs://dataproc-staging-us-central1-198060065126-ydwngrbp/week3-pyspark/input/cleaned_products.csv"

# Load CSV files into DataFrames
orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)
order_products_df = spark.read.csv(order_products_path, header=True, inferSchema=True)
cleaned_products_df = spark.read.csv(cleaned_products_path, header=True, inferSchema=True)

# Register DataFrames as SQL temporary views
orders_df.createOrReplaceTempView("orders")
order_products_df.createOrReplaceTempView("order_products")
cleaned_products_df.createOrReplaceTempView("cleaned_products")

# Run the SQL query
result = spark.sql("""
SELECT cp.product_name, 
       COUNT(op.reordered) AS reorder_product_count
FROM order_products op
JOIN cleaned_products cp 
    ON op.product_id = cp.product_id
WHERE op.reordered = 1
GROUP BY cp.product_name
ORDER BY reorder_product_count DESC
LIMIT 5;
""")

# Show the results
result.show(truncate=False)