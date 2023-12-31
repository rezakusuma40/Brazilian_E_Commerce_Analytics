import psycopg2

# establish connection with 2 postgresql databases, clean db and data mart
clean_conn=psycopg2.connect(user='postgres',
            password='postgres',
            host='localhost',
            port='2024',
            database='cleandb')
clean_cur=clean_conn.cursor()

mart_conn=psycopg2.connect(user='postgres',
            password='postgres',
            host='localhost',
            port='2024',
            database='data_mart')
mart_cur=mart_conn.cursor()

# create data mart tables if not already existed
mart_cur.execute("create table if not exists total_order \
                (count int)")
mart_cur.execute("create table if not exists successful_delivery_percentage \
                (delivered_order_percentage float(4))")
mart_cur.execute("create table if not exists total_sales_of_delivered_order \
                (total_sales float(12))")
mart_cur.execute("create table if not exists average_sales_of_delivered_order \
                (average_sales float(6))")
mart_cur.execute("create table if not exists total_sales_of_delivered_order_per_state \
                (customer_state char(2), total_sales float(12))")
mart_cur.execute("create table if not exists average_shipping_duration \
                (average_shipping_time varchar(16))")
mart_cur.execute("create table if not exists order_delivery_lateness \
                (order_delivery varchar(10), count int)")
mart_cur.execute("create table if not exists top_product_category_by_sales \
                (product_category varchar(50), total_sales float(12), \
                average_rating float(4))")
mart_conn.commit()

# query clean tables to get data for data mart
# save the data to some python object before writing to data mart
clean_cur.execute("select count(order_id) as count from clean_orders co \
    where order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) \
    FROM clean_orders) - interval '6 months'")
df_total_order=clean_cur.fetchall()
clean_cur.execute("SELECT\
    COUNT(*) * 100.0 / (SELECT COUNT(*) \
        FROM clean_orders \
        where order_purchase_timestamp >= \
            (SELECT MAX(order_purchase_timestamp) FROM clean_orders) \
            - interval '6 months') as delivered_order_percentage \
    FROM \
    clean_orders \
    where order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) \
        FROM clean_orders) - interval '6 months' \
        and order_status ='delivered' \
    GROUP BY order_status")
df_successful_delivery_percentage=clean_cur.fetchall()
clean_cur.execute("select sum(coi.price) as total_sales \
    from clean_order_items coi join clean_orders co on coi.order_id = co.order_id \
    where co.order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) \
    FROM clean_orders) - interval '6 months' \
        and co.order_status = 'delivered'")
df_total_sales_of_delivered_order=clean_cur.fetchall()
clean_cur.execute("with sum_price_per_order as ( \
            select coi.order_id, sum(coi.price) as sum_price \
            from clean_order_items coi join clean_orders co on coi.order_id =co.order_id \
            where co.order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) FROM clean_orders) - interval '6 months' \
                and co.order_status = 'delivered' \
            group by coi.order_id) \
        select avg(sum_price) as average_sales from sum_price_per_order")
df_average_sales_of_delivered_order=clean_cur.fetchall()
clean_cur.execute("select cc.customer_state, sum(coi.price) as total_sales \
    from clean_customers cc \
    join clean_orders co on cc.customer_id = co.customer_id \
    join clean_order_items coi on co.order_id = coi.order_id \
    where co.order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) FROM clean_orders) - interval '6 months' \
        and co.order_status = 'delivered' \
    group by cc.customer_state \
    order by total_sales desc")
df_total_sales_of_delivered_order_per_state=clean_cur.fetchall()
clean_cur.execute("with avg_deliv_dur as ( \
        select avg(age(order_delivered_customer_date, order_purchase_timestamp)) as avg_time_diff from clean_orders co \
        where co.order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) FROM clean_orders) - interval '6 months') \
    select EXTRACT(DAY FROM avg_time_diff) || ' days ' || \
        EXTRACT(HOUR FROM avg_time_diff) || ' hours ' AS average_shipping_time \
    from avg_deliv_dur")
df_average_shipping_duration=clean_cur.fetchall()
clean_cur.execute("SELECT \
        'late' AS order_delivery, \
        COUNT(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 END) AS count \
        from clean_orders \
        where order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) FROM clean_orders) - interval '6 months' \
    UNION ALL \
        SELECT \
        'on-time' AS order_delivery, \
        COUNT(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 END) AS count \
        from clean_orders \
        where order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) FROM clean_orders) - interval '6 months'")
df_order_delivery_lateness=clean_cur.fetchall()
clean_cur.execute("WITH ranked_data AS ( \
        SELECT cpcnt.product_category_name_english, sum(coi.price) AS total_sales, \
            avg(cor.review_score) as average_rating, \
            ROW_NUMBER() OVER (ORDER BY sum(coi.price) DESC) AS rank \
        FROM clean_product_category_name_translation cpcnt \
        JOIN clean_products cp ON cpcnt.product_category_name = cp.product_category_name \
        JOIN clean_order_items coi ON cp.product_id = coi.product_id \
        JOIN clean_orders co ON coi.order_id = co.order_id \
        join clean_order_reviews cor on co.order_id= cor.order_id \
        where co.order_status = 'delivered' \
            and co.order_purchase_timestamp >= (SELECT MAX(order_purchase_timestamp) FROM clean_orders) - interval '6 months' \
        GROUP BY cpcnt.product_category_name_english \
    ) \
    SELECT product_category_name_english as product_category, total_sales, average_rating \
    FROM ranked_data \
    WHERE rank <= 10")
df_top_product_category_by_sales=clean_cur.fetchall()
clean_conn.close()

# write to data mart
mart_cur.executemany("INSERT INTO total_order VALUES (%s)", 
    df_total_order)
mart_cur.executemany("INSERT INTO successful_delivery_percentage VALUES (%s)", 
    df_successful_delivery_percentage)
mart_cur.executemany("INSERT INTO total_sales_of_delivered_order VALUES (%s)", 
    df_total_sales_of_delivered_order)
mart_cur.executemany("INSERT INTO average_sales_of_delivered_order VALUES (%s)", 
    df_average_sales_of_delivered_order)
mart_cur.executemany("INSERT INTO total_sales_of_delivered_order_per_state VALUES (%s, %s)",
                     df_total_sales_of_delivered_order_per_state)
mart_cur.executemany("INSERT INTO average_shipping_duration VALUES (%s)", 
    df_average_shipping_duration)
mart_cur.executemany("INSERT INTO order_delivery_lateness VALUES (%s, %s)", \
    df_order_delivery_lateness)
mart_cur.executemany("INSERT INTO top_product_category_by_sales VALUES (%s, %s, %s)", \
    df_top_product_category_by_sales)
mart_cur.execute("ALTER TABLE total_sales_of_delivered_order_per_state \
    ADD COLUMN customer_state_name VARCHAR(20)")
mart_conn.commit()
mart_cur.execute("UPDATE total_sales_of_delivered_order_per_state\
    SET customer_state_name = \
    CASE \
    WHEN customer_state = 'AC' THEN 'ACRE' \
    WHEN customer_state = 'AL' THEN 'ALAGOAS' \
    when customer_state = 'AM' THEN 'AMAZONAS' \
    when customer_state = 'AP' then 'AMAPA' \
    WHEN customer_state = 'BA' THEN 'BAHIA' \
    WHEN customer_state = 'CE' THEN 'CEARA' \
    WHEN customer_state = 'DF' THEN 'DISTRITO FEDERAL' \
    WHEN customer_state = 'ES' THEN 'ESPIRITO SANTO' \
    WHEN customer_state = 'GO' THEN 'GOIAS' \
    WHEN customer_state = 'MA' THEN 'MARANHAO' \
    WHEN customer_state = 'MG' THEN 'MINAS GERAIS' \
    WHEN customer_state = 'MS' THEN 'MATO GROSSO DO SUL' \
    WHEN customer_state = 'MT' THEN 'MATO GROSSO' \
    WHEN customer_state = 'PA' THEN 'PARA' \
    WHEN customer_state = 'PB' THEN 'PARAIBA' \
    WHEN customer_state = 'PE' THEN 'PERNAMBUCO' \
    WHEN customer_state = 'PI' THEN 'PIAUI' \
    WHEN customer_state = 'PR' THEN 'PARANA' \
    WHEN customer_state = 'RJ' THEN 'RIO DE JANEIRO' \
    WHEN customer_state = 'RN' THEN 'RIO GRANDE DO NORTE' \
    WHEN customer_state = 'RO' THEN 'RONDONIA' \
    WHEN customer_state = 'RR' THEN 'RORAIMA' \
    WHEN customer_state = 'RS' THEN 'RIO GRANDE DO SUL' \
    WHEN customer_state = 'SC' THEN 'SANTA CATARINA' \
    WHEN customer_state = 'SE' THEN 'SERGIPE' \
    WHEN customer_state = 'SP' THEN 'SAO PAULO' \
    WHEN customer_state = 'TO' THEN 'TOCANTINS' \
    ELSE customer_state \
    END;")
mart_conn.commit()
mart_conn.close()