import psycopg2
# import findspark
# findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, \
ByteType, ShortType, IntegerType, FloatType, DoubleType, TimestampType

# establish connection with postgresql
raw_conn=psycopg2.connect(user='postgres',
            password='postgres',
            host='localhost',
            port='2024',
            database='rawdb')
raw_cur=raw_conn.cursor()

# create staging database tables if not already existed
raw_cur.execute("create table if not exists raw_customers \
            (customer_id char(32),\
            customer_unique_id char(32),\
            customer_zip_code_prefix char(5),\
            customer_city varchar(40),\
            customer_state char(2))")
raw_cur.execute("create table if not exists raw_geolocation \
            (geolocation_zip_code_prefix char(5),\
            geolocation_lat float(15),\
            geolocation_lng float(15),\
            geolocation_city varchar(40),\
            geolocation_state char(2))")
raw_cur.execute("create table if not exists raw_order_items \
            (order_id char(32),\
            order_item_id smallint,\
            product_id char(32),\
            seller_id char(32),\
            shipping_limit_date timestamp,\
            price float(2),\
            freight_value float(2))")
raw_cur.execute("create table if not exists raw_order_payments \
            (order_id char(32),\
            payment_sequential int,\
            payment_type varchar(20),\
            payment_installments int,\
            payment_value float(2))")
raw_cur.execute("create table if not exists raw_order_reviews \
            (review_id char(32),\
            order_id char(32),\
            review_score smallint,\
            review_comment_title varchar(255),\
            review_comment_message varchar(255),\
            review_creation_date timestamp,\
            review_answer_timestamp timestamp)")
raw_cur.execute("create table if not exists raw_orders \
            (order_id char(32),\
            customer_id char(32),\
            order_status varchar(11),\
            order_purchase_timestamp timestamp,\
            order_approved_at timestamp,\
            order_delivered_carrier_date timestamp,\
            order_delivered_customer_date timestamp,\
            order_estimated_delivery_date timestamp)")
raw_cur.execute("create table if not exists raw_product_category_name_translation \
            (product_category_name varchar(50),\
            product_category_name_english varchar(50))")
raw_cur.execute("create table if not exists raw_products \
            (product_id char(32),\
            product_category_name varchar(50),\
            product_name_lenght smallint,\
            product_description_lenght smallint,\
            product_photos_qty smallint,\
            product_weight_g int,\
            product_length_cm smallint,\
            product_height_cm smallint,\
            product_width_cm smallint)")
raw_cur.execute("create table if not exists raw_sellers \
            (seller_id char(32),\
            seller_zip_code_prefix char(5),\
            seller_city varchar(40),\
            seller_state char(2))")
raw_conn.commit()
raw_conn.close()

# create spark session
spark=SparkSession\
        .builder\
        .appName('ecommerce_cilsy_rawdb_design')\
        .config("spark.jars", "C:\spark-3.2.3-bin-hadoop3.2\jars\postgresql-42.6.0.jar")\
        .getOrCreate()

# load data from raw csv files to pyspark
schema = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("customer_unique_id", StringType(), nullable=True),
    StructField("customer_zip_code_prefix", StringType(), nullable=True),
    StructField("customer_city", StringType(), nullable=True),
    StructField("customer_state", StringType(), nullable=True)
    ])
df_raw_customers=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\customers_dataset.csv', schema=schema)

schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), nullable=True),
    StructField("geolocation_lat", DoubleType(), nullable=True),
    StructField("geolocation_lng", DoubleType(), nullable=True),
    StructField("geolocation_city", StringType(), nullable=True),
    StructField("geolocation_state", StringType(), nullable=True)
    ])
df_raw_geolocation=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\geolocation_dataset.csv', schema=schema)

schema = StructType([
    StructField("order_id", StringType(), nullable=True),
    StructField("order_item_id", ByteType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("seller_id", StringType(), nullable=True),
    StructField("shipping_limit_date", TimestampType(), nullable=True),
    StructField("price", FloatType(), nullable=True),
    StructField("freight_value", FloatType(), nullable=True)
    ])
df_raw_order_items=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\order_items_dataset.csv', schema=schema)

schema = StructType([
    StructField("order_id", StringType(), nullable=True),
    StructField("payment_sequential", ByteType(), nullable=True),
    StructField("payment_type", StringType(), nullable=True),
    StructField("payment_installments", IntegerType(), nullable=True),
    StructField("payment_value", FloatType(), nullable=True)
    ])
df_raw_order_payments=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\order_payments_dataset.csv', schema=schema)

schema = StructType([
    StructField("review_id", StringType(), nullable=True),
    StructField("order_id", StringType(), nullable=True),
    StructField("review_score", ByteType(), nullable=True),
    StructField("review_comment_title", StringType(), nullable=True),
    StructField("review_comment_message", StringType(), nullable=True),
    StructField("review_creation_date", TimestampType(), nullable=True),
    StructField("review_answer_timestamp", TimestampType(), nullable=True)
    ])
#need to use miltiline since this file is kinda broken
df_raw_order_reviews=spark.read\
.option("header",True)\
.option("multiline",True)\
.csv('D:\project\ecommerce_brazil\data_raw\order_reviews_dataset.csv', schema=schema)

schema = StructType([
    StructField("order_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
    StructField("order_purchase_timestamp", TimestampType(), nullable=True),
    StructField("order_approved_at", TimestampType(), nullable=True),
    StructField("order_delivered_carrier_date", TimestampType(), nullable=True),
    StructField("order_delivered_customer_date", TimestampType(), nullable=True),
    StructField("order_estimated_delivery_date", TimestampType(), nullable=True)
    ])
df_raw_orders=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\orders_dataset.csv', schema=schema)

schema = StructType([
    StructField("product_category_name", StringType(), nullable=True),
    StructField("product_category_name_english", StringType(), nullable=True),
    ])
df_raw_product_category_name_translation=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\product_category_name_translation.csv', schema=schema)

schema = StructType([
    StructField("product_id", StringType(), nullable=True),
    StructField("product_category_name", StringType(), nullable=True),
    StructField("product_name_lenght", ShortType(), nullable=True),
    StructField("product_description_lenght", ShortType(), nullable=True),
    StructField("product_photos_qty", ShortType(), nullable=True),
    StructField("product_weight_g", IntegerType(), nullable=True),
    StructField("product_length_cm", ShortType(), nullable=True),
    StructField("product_height_cm", ShortType(), nullable=True),
    StructField("product_width_cm", ShortType(), nullable=True)
    ])
df_raw_products=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\products_dataset.csv', schema=schema)

schema = StructType([
    StructField("seller_id", StringType(), nullable=True),
    StructField("seller_zip_code_prefix", StringType(), nullable=True),
    StructField("seller_city", StringType(), nullable=True),
    StructField("seller_state", StringType(), nullable=True)
    ])
df_raw_sellers=spark.read.option("header",True)\
.csv('D:\project\ecommerce_brazil\data_raw\sellers_dataset.csv', schema=schema)

# overwrite spark df to staging database in postgresql
df_raw_customers.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_customers') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_raw_geolocation.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_geolocation') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_raw_order_items.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_order_items') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_raw_order_payments.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_order_payments') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
#some values gone missing for no reason, we'll drop those missing values
df_without_missing = df_raw_order_reviews.na.drop(subset=["review_creation_date",
                                                         "review_answer_timestamp"])
df_without_missing.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_order_reviews') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_raw_orders.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_orders') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_raw_product_category_name_translation.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_product_category_name_translation') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_raw_products.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_products') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_raw_sellers.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_sellers') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .option('driver','org.postgresql.Driver') \
        .save()