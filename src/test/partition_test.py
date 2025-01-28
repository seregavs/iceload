from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
import spark_const_test

database = 'db'
sparkdb = "{0}.{1}".format(spark_const_test.spark_catalog, database)

spark = SparkSession.builder.master("local[2]").config(conf=spark_const_test.conf_2g_ice_warehouse2).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

def proc1():
    li = '/home/alpine/iceload/data/cmlc7o99/t1/cmlc70993_20241220.csv'
    dst = '/home/alpine/iceload/data/cmlc7o99/t/cmlc70993'
    delimiter = ','
    ## https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html
    spark.sql("SET spark.sql.ansi.enabled=true").show(10)
    df = spark.read.options(delimiter=delimiter, header=True, escape="\\").csv(li)
    df.select('REQTSN').distinct().show()
    df.write.mode('overwrite').partitionBy('REQTSN').parquet(dst)
    # df.show(4)
    # df.show(5, truncate=False)
    spark.stop()


def proc1a():
    li = '/home/alpine/iceload/data/cmlc7o99/t1/cmlc70993_20241220.csv'
    dst = '/home/alpine/iceload/data/cmlc7o99/t/cmlc70993'
    delimiter = ','
    # https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html
    spark.sql("SET spark.sql.ansi.enabled=true").show(10)
    df = spark.read.options(delimiter=delimiter, header=True, escape="\\").csv(li)
    df.select('REQTSN').distinct().show()
    print(df.count())
    df.write.mode('overwrite').partitionBy('REQTSN').orc(dst)
    # df.show(4)
    # df.show(5, truncate=False)
    spark.stop()


def proc2():
    lst = []
    lst.append('REQTS=20241220012631000086000')
    lst.append('REQTS=20241220012859000030000')
    lst.append('REQTS=20241220015858000147000')
    lst.append('REQTS=20241220234808000217000')
    lst.append('REQTS=20241220235557000127000')
    cnt = 0
    for li in lst:
        # fi = f'/home/alpine/iceload/data/cmlc7o99/t/cmlc70993/{li}'
        fi = f'/home/alpine/iceload/data/cmlc7o99/{li}'
        df = spark.read.parquet(fi)
        cnt += df.count()
        print(f'{li}, running total: {cnt}')
        # df.select('PLANT').distinct().show()
        # df.show(4)
    spark.stop()


proc1a()
