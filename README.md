In this project I created a dashboard about brazilian e-commerce sales.

### Workflow:

1. Create staging database.
2. Read raw data from several csv files.
3. Clean multilines so there won't be error when storing data to staging database.
4. Store data to staging database.
5. Create clean database.
6. Load data from staging database.
7. Clean data, normalize it, assign keys.
8. Write to Clean database.
9. Create data marts.
10. Select and aggregate data from clean database using SQL queries and store the result in data marts.
11. Visualize data.

The process indeed looked convoluted, it was because in this project I did a lot of experiments for learning purpose.

The database creation, ETL process, and SQL Querying were all done with python using SparkSQL and psycopg2. Although 1 is sufficient but I did it this way since I wanted to experience using both. Other than that there are other things I experimented:

1. Running PostgreSQL with docker.
2. Using PowerBI to create dashboard.
"# Brazilian_E_Commerce_Analytics" 
