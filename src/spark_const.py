from pyspark import SparkConf
import os

# Everything about spark catalogues
# https://iceberg.apache.org/docs/1.5.1/spark-configuration/#catalog-configuration

spark_catalog = 'local'
spark_catalog_path = '/home/alpine/spark-warehouse2'

conf_2g_ice_warehouse2 = (SparkConf()
            .setAppName("Iceberg app")
            .set("spark.driver.memory", "2g")
            .set("spark.ui.port", "4046")
            .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
            .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.defaultCatalog", f"{spark_catalog}")
            .set(f"spark.sql.catalog.{spark_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .set(f"spark.sql.catalog.{spark_catalog}.type", "hadoop")
            .set(f"spark.sql.catalog.{spark_catalog}.warehouse", f"{spark_catalog_path}")
            .set(f"spark.sql.catalog.{spark_catalog}.table-default.write.metadata.delete-after-commit.enabled", "true")
            .set(f"spark.sql.catalog.{spark_catalog}.write.metadata.previous-versions-max", "5")
        )

DB_HOST = os.environ.get("PG_HOST") if os.environ.get("PG_HOST") else 'localhost'
DB_PORT = os.environ.get("PG_PORT") if os.environ.get("PG_PORT") else '5432'
DB_NAME = os.environ.get("PG_DB_CLEAN") if os.environ.get("PG_DB_CLEAN") else 'alpdb'
DB_PASSWORD = os.environ.get("PG_PASSWORD") if os.environ.get("PG_PASSWORD") else 'Init1234_5'
DB_USER = os.environ.get("PG_USERNAME") if os.environ.get("PG_USERNAME") else 'joe'

jdbc_postgre_alpdb_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
jdbc_postgre_alpdb_props = {"user": DB_USER,
                            "password": DB_PASSWORD,
                            "driver": "org.postgresql.Driver",
                            "fetchsize": "1000"}
