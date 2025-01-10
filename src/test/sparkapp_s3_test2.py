from pyspark.sql import SparkSession

 
spark = SparkSession.builder.appName("Spark From S3")\
    .config("spark.hadoop.fs.s3a.access.key", yc_access_key_id)\
    .config("spark.hadoop.fs.s3a.secret.key", yc_secret_access_key)\
    .config("spark.hadoop.fs.s3a.endpoint", yc_endpoint)\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# read from s3
df_s3 = spark.read.csv("s3a://stg-bi-1/vendor/main.csv", header=False)
df_s3.show()
# df_s3 = spark.read.orc("s3a://stg-bi-1/comp_code/part-00000-6eb07e49-6284-40cc-b8be-2a231cc83201-c000")
# df_s3.show(10, truncate=False)
spark.stop()

# write to s3
# df_to_s3 = spark.sql("SELECT 1 as id, 'odin' as name")
# df_to_s3.write.mode('append').format("csv").save("s3a://s3objectstoragetestspark/testspark_in/")