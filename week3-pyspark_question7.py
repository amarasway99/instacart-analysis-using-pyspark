from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Order Period Analysis").getOrCreate()

# Define the file paths
orders_path = "gs://dataproc-staging-us-central1-198060065126-ydwngrbp/week3-pyspark/input/orders.csv"
order_products_path = "gs://dataproc-staging-us-central1-198060065126-ydwngrbp/week3-pyspark/input/order_products.csv"
cleaned_products_path = "gs://dataproc-staging-us-central1-198060065126-ydwngrbp/week3-pyspark/input/cleaned_products.csv"
departments_path = "gs://dataproc-staging-us-central1-198060065126-ydwngrbp/week3-pyspark/input/departments.csv"

# Load CSV files into DataFrames
orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)
order_products_df = spark.read.csv(order_products_path, header=True, inferSchema=True)
cleaned_products_df = spark.read.csv(cleaned_products_path, header=True, inferSchema=True)
departments_df = spark.read.csv(departments_path, header=True, inferSchema=True)

# Register DataFrames as SQL temporary views
orders_df.createOrReplaceTempView("orders")
order_products_df.createOrReplaceTempView("order_products")
cleaned_products_df.createOrReplaceTempView("cleaned_products")
departments_df.createOrReplaceTempView("departments")

# Run the SQL query
result = spark.sql("""
    SELECT
    d.department,
    SUM(CASE WHEN o.order_hour_of_day BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS overnight_orders,
    SUM(CASE WHEN o.order_hour_of_day BETWEEN 6 AND 12 THEN 1 ELSE 0 END) AS morning_orders,
    SUM(CASE WHEN o.order_hour_of_day BETWEEN 13 AND 17 THEN 1 ELSE 0 END) AS afternoon_orders, 
    SUM(CASE WHEN o.order_hour_of_day BETWEEN 18 AND 23 THEN 1 ELSE 0 END) AS evening_orders
    FROM orders o
    JOIN order_products op ON o.order_id = op.order_id
    JOIN cleaned_products p ON op.product_id = p.product_id
    JOIN departments d ON p.department_id = d.department_id
    GROUP BY d.department
    ORDER BY d.department;
""")

# Show the results
result.show(truncate=False)