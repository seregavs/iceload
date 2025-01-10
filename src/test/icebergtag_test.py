from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
import spark_const_test
import yaml


def main():
    database = 'db'
    sparkdb = "{0}.{1}".format(spark_const_test.spark_catalog, database)

    spark = SparkSession.builder.master("local[2]").config(conf=spark_const_test.conf_2g_ice_warehouse2).getOrCreate()

    icebergtbl_props = "/home/alpine/iceload/src/icebergtbl_props.yaml"

    with open(icebergtbl_props, "r") as f2:
        tbl_props_params = yaml.safe_load(f2)
    tbl_props = tbl_props_params['iceberg_tbl_props_01']

    query = """ CREATE OR REPLACE TABLE {0}.dwh_t_tagdemo (id STRING  NOT NULL, amount DECIMAL(17,2)) USING iceberg TBLPROPERTIES {1}""". \
        format(sparkdb, tbl_props)
    spark.sql(query).show(30, truncate=False)
    query = """INSERT INTO {0}.dwh_t_tagdemo VALUES(1, 10)""".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = """ALTER TABLE {0}.dwh_t_tagdemo CREATE OR REPLACE TAG `historical-tag`""".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = "SELECT * FROM {0}.dwh_t_tagdemo ORDER BY 1".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = """INSERT INTO {0}.dwh_t_tagdemo VALUES(2, 20)""".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = """ALTER TABLE {0}.dwh_t_tagdemo CREATE OR REPLACE TAG `latest-tag`""".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = "SELECT * FROM {0}.dwh_t_tagdemo VERSION AS OF 'historical-tag'".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = "SELECT v1.* FROM ({0}.dwh_t_tagdemo VERSION AS OF 'latest-tag') as v1 ORDER BY 1 ASC ".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = "SELECT * FROM {0}.dwh_t_tagdemo".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    query = """ALTER TABLE {0}.dwh_t_tagdemo DROP TAG `latest-tag`""".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    # Удаление tag не удаляет данные и они остаются в SELECT :(
    query = "SELECT * FROM {0}.dwh_t_tagdemo".format(sparkdb)
    spark.sql(query).show(30, truncate=False)
    spark.stop()


if __name__ == '__main__':
    main()
