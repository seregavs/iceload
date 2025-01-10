from pyspark.sql import SparkSession
from pyspark import SparkConf

 
conf_2g_s3_warehouse = SparkConf()\
    .setAppName("s3-spark")\
    .set("spark.driver.memory", "2g")\
    .set("spark.ui.port", "4046")\
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")


spark = SparkSession.builder.master("local[2]")\
            .config(conf=conf_2g_s3_warehouse)\
            .config("spark.hadoop.fs.s3a.access.key", yc_access_key_id)\
            .config("spark.hadoop.fs.s3a.secret.key", yc_secret_access_key)\
            .config("spark.hadoop.fs.s3a.endpoint", yc_endpoint)\
            .config("spark.hadoop.fs.s3a.endpoint.region", yc_region_name)\
            .getOrCreate()

# spark1 = SparkSession.builder.master("local[2]")\
#     .config("spark.hadoop.fs.s3a.access.key", yc_access_key_id)\
#     .config("spark.hadoop.fs.s3a.secret.key", yc_secret_access_key)\
#     .config("spark.hadoop.fs.s3a.endpoint", yc_endpoint)\
#     .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# read from s3
df_s3 = spark.read.orc("s3a://stg-bi-1/comp_code/part-00000-6eb07e49-6284-40cc-b8be-2a231cc83201-c000")
df_s3.show(10, truncate=False)
spark.stop()
