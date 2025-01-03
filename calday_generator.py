from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, date_format, year, month, dayofweek, quarter, sequence, expr
from datetime import date
import spark_const
import yaml

# Initialize Spark Session
spark = SparkSession.builder.master("local[2]").config(conf=spark_const.conf_2g_ice_warehouse2).getOrCreate()
spark.conf.set("spark.sql.session.locale", "ru")  # Не работает :(

# INITIALIZATION
start_date = date(2000, 1, 1)
end_date = date(2030, 12, 31)
src_table_name = 'stg_t_calday'
with open("icebergtbl_props.yaml", "r") as f2:
    tbl_props_params = yaml.safe_load(f2)
tbl_props = tbl_props_params['iceberg_tbl_props_01']
table_name = 'sys_t_calday'
database = 'db'
sparkdb = "{0}.{1}".format(spark_const.spark_catalog, database)

# Generate the sequence of dates
calendar_df = spark.sql("""
    SELECT explode(sequence(to_date('{}'), to_date('{}'), interval 1 day)) AS date
""".format(start_date, end_date))

# Add analytics columns
calendar_df = calendar_df.withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("day_of_week", expr("CASE WHEN dayofweek(date) = 1 THEN 7 ELSE dayofweek(date) - 1 END")) \
    .withColumn("weekend", when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False))) \
    .withColumn("quarter", quarter(col("date")))

calendar_df = calendar_df.withColumn("day_name", date_format(col("date"), "EEEE"))
calendar_df.createOrReplaceTempView(src_table_name)

query = """CREATE OR REPLACE TABLE {0}.{2} (
                calday DATE NOT NULL,
                calday_s STRING  NOT NULL,
                calyear INTEGER  NOT NULL,
                calquarter1 INTEGER  NOT NULL, 
                calmonth2 INTEGER  NOT NULL, 
                weekday1 INTEGER  NOT NULL, 
                weekday1_txt STRING  NOT NULL, 
                weekend BOOLEAN  NOT NULL, 
                holiday BOOLEAN  NOT NULL,
                calmonth STRING  NOT NULL,
                calquarter STRING  NOT NULL
               ) USING iceberg TBLPROPERTIES {1}
               """.format(sparkdb, tbl_props, table_name)
spark.sql(query)
query = '''ALTER TABLE {1}.{0} SET IDENTIFIER FIELDS {2}'''.format(table_name, sparkdb, 'calday')
spark.sql(query)
query = '''INSERT INTO {0}.{1}
                    SELECT
                          date as calday
                        , date_format(date,'yyyyMMdd') as calday_s
                        , year as calyear
                        , quarter as calquarter1
                        , month as calmonth2
                        , day_of_week as weekday1
                        , day_name as weekday1_txt
                        , weekend
                        , False as holiday
                        , year || lpad(month,2,'0') as calmonth
                        , year || lpad(quarter,2,'0') as calquarter
                        from {2}
                order by date asc
        '''.format(sparkdb, table_name, src_table_name)
spark.sql(query)
query = '''SELECT * FROM {0}.{1} ORDER BY 1 ASC'''.format(sparkdb, table_name)
df = spark.sql(query)
df.show(15, truncate=False)
# df.printSchema()
