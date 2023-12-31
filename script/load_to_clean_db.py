import psycopg2
# import findspark
# findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# establish connection with postgresql
clean_conn=psycopg2.connect(user='postgres',
            password='postgres',
            host='localhost',
            port='2024',
            database='cleandb')
clean_cur=clean_conn.cursor()

# create clean database tables if not already existed
# doesn't need to change much since the original data is already pretty structured
# only add 1 additional table for storing unique zip 
clean_cur.execute("create table if not exists clean_customers \
            (customer_id char(32) PRIMARY KEY,\
            customer_unique_id char(32),\
            customer_zip_code_prefix char(5),\
            customer_city varchar(40),\
            customer_state char(2))")
clean_cur.execute("create table if not exists clean_geolocation \
            (geolocation_id serial PRIMARY KEY,\
            geolocation_zip_code_prefix char(5),\
            geolocation_lat float(15),\
            geolocation_lng float(15),\
            geolocation_city varchar(40),\
            geolocation_state char(2))")
clean_cur.execute("create table if not exists clean_zip \
            (zip_code_prefix char(5),\
            zip_city varchar(40),\
            zip_state char(2),\
            PRIMARY KEY (zip_code_prefix, zip_city, zip_state))")
clean_cur.execute("create table if not exists clean_order_items \
            (order_id char(32),\
            order_item_id smallint,\
            product_id char(32),\
            seller_id char(32),\
            shipping_limit_date timestamp,\
            price float(2),\
            freight_value float(2),\
            PRIMARY KEY (order_id, order_item_id))")
clean_cur.execute("create table if not exists clean_order_payments \
            (order_id char(32),\
            payment_sequential int,\
            payment_type varchar(20),\
            payment_installments int,\
            payment_value float(2),\
            PRIMARY KEY (order_id, payment_sequential))")
clean_cur.execute("create table if not exists clean_order_reviews \
            (review_id char(32),\
            order_id char(32),\
            review_score smallint,\
            review_comment_title varchar(255),\
            review_comment_message varchar(255),\
            review_creation_date timestamp,\
            review_answer_timestamp timestamp,\
            PRIMARY KEY (order_id, review_id))")
clean_cur.execute("create table if not exists clean_orders \
            (order_id char(32) PRIMARY KEY,\
            customer_id char(32),\
            order_status varchar(11),\
            order_purchase_timestamp timestamp,\
            order_approved_at timestamp,\
            order_delivered_carrier_date timestamp,\
            order_delivered_customer_date timestamp,\
            order_estimated_delivery_date timestamp)")
clean_cur.execute("create table if not exists clean_product_category_name_translation \
            (product_category_name varchar(50) PRIMARY KEY,\
            product_category_name_english varchar(50))")
clean_cur.execute("create table if not exists clean_products \
            (product_id char(32) PRIMARY KEY,\
            product_category_name varchar(50),\
            product_name_lenght smallint,\
            product_description_lenght smallint,\
            product_photos_qty smallint,\
            product_weight_g int,\
            product_length_cm smallint,\
            product_height_cm smallint,\
            product_width_cm smallint)")
clean_cur.execute("create table if not exists clean_sellers \
            (seller_id char(32) PRIMARY KEY,\
            seller_zip_code_prefix char(5),\
            seller_city varchar(40),\
            seller_state char(2))")

# add keys
clean_cur.execute("ALTER TABLE clean_customers\
            ADD CONSTRAINT fk_clean_customers_to_clean_zip\
            FOREIGN KEY (customer_zip_code_prefix, customer_city, customer_state)\
            REFERENCES clean_zip (zip_code_prefix, zip_city, zip_state)")
clean_cur.execute("ALTER TABLE clean_geolocation\
            ADD CONSTRAINT fk_clean_geolocation_to_clean_zip\
            FOREIGN KEY (geolocation_zip_code_prefix, geolocation_city, geolocation_state)\
            REFERENCES clean_zip (zip_code_prefix, zip_city, zip_state)")
clean_cur.execute("ALTER TABLE clean_order_items\
            ADD CONSTRAINT fk_clean_order_items_to_clean_orders\
            FOREIGN KEY (order_id)\
            REFERENCES clean_orders (order_id)")
clean_cur.execute("ALTER TABLE clean_order_items\
            ADD CONSTRAINT fk_clean_order_items_to_clean_products\
            FOREIGN KEY (product_id)\
            REFERENCES clean_products (product_id)")
clean_cur.execute("ALTER TABLE clean_order_items\
            ADD CONSTRAINT fk_clean_order_items_to_clean_sellers\
            FOREIGN KEY (seller_id)\
            REFERENCES clean_sellers (seller_id)")
clean_cur.execute("ALTER TABLE clean_order_payments\
            ADD CONSTRAINT fk_clean_order_payments_to_clean_orders\
            FOREIGN KEY (order_id)\
            REFERENCES clean_orders (order_id)")
clean_cur.execute("ALTER TABLE clean_order_reviews\
            ADD CONSTRAINT fk_clean_order_reviews_to_clean_orders\
            FOREIGN KEY (order_id)\
            REFERENCES clean_orders (order_id)")
clean_cur.execute("ALTER TABLE clean_orders\
            ADD CONSTRAINT fk_clean_orders_to_clean_customers\
            FOREIGN KEY (customer_id)\
            REFERENCES clean_customers (customer_id)")
clean_cur.execute("ALTER TABLE clean_products\
            ADD CONSTRAINT fk_clean_products_to_clean_product_category_name_translation\
            FOREIGN KEY (product_category_name)\
            REFERENCES clean_product_category_name_translation (product_category_name)")
clean_cur.execute("ALTER TABLE clean_sellers\
            ADD CONSTRAINT fk_clean_sellers_to_clean_zip\
            FOREIGN KEY (seller_zip_code_prefix, seller_city, seller_state)\
            REFERENCES clean_zip (zip_code_prefix, zip_city, zip_state)")
clean_conn.commit()
clean_conn.close()

# create spark session
spark=SparkSession\
        .builder\
        .appName('ecommerce_cilsy_cleandb_design')\
        .config("spark.jars", "C:\spark-3.2.3-bin-hadoop3.2\jars\postgresql-42.6.0.jar")\
        .getOrCreate()

# load data from raw db to preprocess them
# nothing much to do since they already almost clean
# zip table is preprocessed from geolocation
# geolocation table will still be made and will have a surrogate key
df_clean_customers=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_customers') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_geolocation=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_geolocation') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_order_items=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_order_items') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_order_payments=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_order_payments') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_order_reviews=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_order_reviews') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_orders=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_orders') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_product_category_name_translation=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_product_category_name_translation') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_products=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_products') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_sellers=spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/rawdb') \
        .option('dbtable', 'raw_sellers') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .option('driver','org.postgresql.Driver') \
        .load()
df_clean_geolocation_sorted=df_clean_geolocation.dropDuplicates() \
        .orderBy('geolocation_zip_code_prefix',
                 'geolocation_city',
                 'geolocation_state',
                 'geolocation_lat',
                 'geolocation_lng')
df_clean_zip=df_clean_geolocation_sorted\
        .drop('geolocation_lat','geolocation_lng')\
        .dropDuplicates()\
        .withColumnRenamed('geolocation_zip_code_prefix','zip_code_prefix')\
        .withColumnRenamed('geolocation_city','zip_city')\
        .withColumnRenamed('geolocation_state','zip_state')

# add data which value exist in the foreign column but don't 
# exist in it's unique reference to the referenced table
unlisted_product_category_name=df_clean_products.join(df_clean_product_category_name_translation, 
    "product_category_name", "leftanti")\
    .select('product_category_name')\
    .distinct()\
    .dropna()\
    .select("product_category_name", lit(None).alias("product_category_name_english"))
df_cleaner_product_category_name_translation=df_clean_product_category_name_translation\
    .unionAll(unlisted_product_category_name)

unlisted_product_id=df_clean_order_items.join(df_clean_products, 
    "product_id", "leftanti")\
    .select('product_id')\
    .distinct()\
    .dropna()\
    .select("product_id", 
            lit(None).alias("product_category_name"),
            lit(None).alias("product_name_lenght"),
            lit(None).alias("product_description_lenght"),
            lit(None).alias("product_photos_qty"),
            lit(None).alias("product_weight_g"),
            lit(None).alias("product_length_cm"),
            lit(None).alias("product_height_cm"),
            lit(None).alias("product_width_cm"),
           )
df_cleaner_products=df_clean_products\
    .unionAll(unlisted_product_id)

unlisted_seller_id=df_clean_order_items.join(df_clean_sellers, 
    "seller_id", "leftanti")\
    .select('seller_id')\
    .distinct()\
    .dropna()\
    .select("seller_id", 
            lit(None).alias("seller_zip_code_prefix"),
            lit(None).alias("seller_city"),
            lit(None).alias("seller_state"),
           )
df_cleaner_sellers=df_clean_sellers\
    .unionAll(unlisted_seller_id)

unlisted_customer_id=df_clean_orders.join(df_clean_customers, 
    "customer_id", "leftanti")\
    .select('customer_id')\
    .distinct()\
    .dropna()\
    .select("customer_id", 
            lit(None).alias("customer_unique_id"),
            lit(None).alias("customer_zip_code_prefix"),
            lit(None).alias("customer_city"),
            lit(None).alias("customer_state"),
           )
df_cleaner_customers=df_clean_customers\
    .unionAll(unlisted_customer_id)

unlisted_zip_code_from_customers=df_clean_customers.join(df_clean_zip, 
    (df_clean_customers["customer_zip_code_prefix"] == df_clean_zip["zip_code_prefix"]) & \
    (df_clean_customers["customer_city"] == df_clean_zip["zip_city"]) & \
    (df_clean_customers["customer_state"] == df_clean_zip["zip_state"]), \
    "leftanti")\
    .select('customer_zip_code_prefix','customer_city','customer_state')\
    .distinct()\
    .dropna()\
    .withColumnRenamed('customer_zip_code_prefix','zip_code_prefix')\
    .withColumnRenamed('customer_city','zip_city')\
    .withColumnRenamed('customer_state','zip_state')
df_cleaner_zip=df_clean_zip\
    .unionAll(unlisted_zip_code_from_customers)

unlisted_zip_code_from_sellers=df_clean_sellers.join(df_cleaner_zip, 
    (df_clean_sellers["seller_zip_code_prefix"] == df_cleaner_zip["zip_code_prefix"]) & \
    (df_clean_sellers["seller_city"] == df_cleaner_zip["zip_city"]) & \
    (df_clean_sellers["seller_state"] == df_cleaner_zip["zip_state"]), \
    "leftanti")\
    .select('seller_zip_code_prefix','seller_city','seller_state')\
    .distinct()\
    .dropna()\
    .withColumnRenamed('seller_zip_code_prefix','zip_code_prefix')\
    .withColumnRenamed('seller_city','zip_city')\
    .withColumnRenamed('seller_state','zip_state')
df_cleanest_zip=df_cleaner_zip\
    .unionAll(unlisted_zip_code_from_sellers)

unlisted_order_id_from_order_items=df_clean_order_items.join(df_clean_orders, 
    "order_id", "leftanti")\
    .select('order_id')\
    .distinct()\
    .dropna()\
    .select("order_id", 
            lit(None).alias("customer_id"),
            lit(None).alias("order_status"),
            lit(None).alias("order_purchase_timestamp"),
            lit(None).alias("order_approved_at"),
            lit(None).alias("order_delivered_carrier_date"),
            lit(None).alias("order_delivered_customer_date"),
            lit(None).alias("order_estimated_delivery_date"),
           )
df_cleaner_orders=df_clean_orders\
    .unionAll(unlisted_order_id_from_order_items)

unlisted_order_id_from_order_payments=df_clean_order_payments.join(df_cleaner_orders, 
    "order_id", "leftanti")\
    .select('order_id')\
    .distinct()\
    .dropna()\
    .select("order_id", 
            lit(None).alias("customer_id"),
            lit(None).alias("order_status"),
            lit(None).alias("order_purchase_timestamp"),
            lit(None).alias("order_approved_at"),
            lit(None).alias("order_delivered_carrier_date"),
            lit(None).alias("order_delivered_customer_date"),
            lit(None).alias("order_estimated_delivery_date"),
           )
df_cleanest_orders=df_cleaner_orders\
    .unionAll(unlisted_order_id_from_order_payments)

unlisted_order_id_from_order_reviews=df_clean_order_reviews.join(df_cleanest_orders, 
    "order_id", "leftanti")\
    .select('order_id')\
    .distinct()\
    .dropna()\
    .select("order_id", 
            lit(None).alias("customer_id"),
            lit(None).alias("order_status"),
            lit(None).alias("order_purchase_timestamp"),
            lit(None).alias("order_approved_at"),
            lit(None).alias("order_delivered_carrier_date"),
            lit(None).alias("order_delivered_customer_date"),
            lit(None).alias("order_estimated_delivery_date"),
           )
df_cleanest2_orders=df_cleanest_orders\
    .unionAll(unlisted_order_id_from_order_reviews)

# append spark df to clean database in postgresql
df_cleaner_product_category_name_translation.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_product_category_name_translation') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_cleaner_products.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_products') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_cleanest_zip.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_zip') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_clean_geolocation_sorted.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_geolocation') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_cleaner_customers.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_customers') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_cleanest2_orders.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_orders') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_clean_order_payments.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_order_payments') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_clean_order_reviews.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_order_reviews') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_cleaner_sellers.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_sellers') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()
df_clean_order_items.write \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost:2024/cleandb') \
        .option('dbtable', 'clean_order_items') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('append') \
        .option('driver','org.postgresql.Driver') \
        .save()