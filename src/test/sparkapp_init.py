from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
import spark_const_test
import yaml

database = 'db'
sparkdb = "{0}.{1}".format(spark_const_test.spark_catalog, database)

spark = SparkSession.builder.master("local[2]").config(conf=spark_const_test.conf_2g_ice_warehouse2).getOrCreate()
print(spark.sql("SELECT * FROM local.db.dwh_t_plant").count())
quit()

li = '/home/alpine/etlakehouse/data/cmlc07p331/CMLC07P331_20241225.csv'

delimiter = ','
## https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html
# spark.sql("SET spark.sql.ansi.enabled=true").show(10)
# df = spark.read.options(delimiter=delimiter, header=True, escape="\\").csv(li)
# df.printSchema()
# df.show(5, truncate=False)
# dst_file = '/home/alpine/etlakehouse/data/cmlc07p331/cmlc07p331_20241225.parquet'
# df.write.mode('overwrite').parquet(dst_file)
# df = spark.read.parquet(dst_file)
# df.printSchema()
# df.show(5, truncate=False)
# quit()
# spark.sql("SELECT plant FROM local.db.dwh_t_plant ORDER BY 1").show(1000, truncate=False)

df = spark.sql("SELECT * FROM {0}.dwh_t_cmlc07p33{1}".format(sparkdb, '2'))
# df.printSchema()
# df.show(10, truncate=False)
print('BEFORE DELETE ' + str(df.count()))
spark.sql("DELETE FROM {0}.dwh_t_cmlc07p33{1} WHERE plant > '0020'".format(sparkdb, '2')).show(10)
df = spark.sql("SELECT * FROM {0}.dwh_t_cmlc07p33{1}".format(sparkdb, '2'))
print('AFTER DELETE ' + str(df.count()))
quit()
# ***************************************************************

# spark.sql('''SHOW CREATE TABLE db.dwh_t_{0}'''.format(li['t'])).show(100, truncate=False)

# df = df.filter("plant = '0302'")
# df = df.filter("VALUE_LC <> 'RUB'")
# df = df.orderBy(asc("REQTSN"), asc("DATAPAKID"), asc("RECORD"), asc("MAT_DOC"), asc("DOC_YEAR"), asc("MAT_ITEM"))

# df.write.mode('overwrite').parquet("/home/alpine/etlakehouse/data/cmlc70993_20241220.parquet")
# df = spark.read.parquet("/home/alpine/etlakehouse/data/cmlc70993_20241220.parquet")

# df.createOrReplaceTempView('TTTT')
# print(df.count())
# spark.sql("SET spark.sql.ansi.enabled=true").show(10)
# df = spark.sql("SELECT * FROM local.db.dwh_t_cmlc099")
# df.printSchema()
#         pstng_date, 
#         doc_date, 
#         calday, 
#         createdon, 
#         gi_date, 

# df = spark.sql("""SELECT REQTSN, MAT_DOC, MAT_ITEM, VALUE_LC, to_number(VALUE_LC,'S999999999999.99') as sss
#                   , CAST(VALUE_LC AS DECIMAL(17,2))  as ssss
#                   , ifnull(STOR_LOC,chr(0)) as STOR_LOC1
#                 FROM TTTT """)
# query = """SELECT DISTINCT VALUE_LC FROM TTTT ORDER BY 1 DESC"""
# VALUE_LC = 'RUB'
# query = '''DROP TABLE {0}.dwh_t_cmlc099 PURGE'''.format(sparkdb)
# query = """SELECT * FROM TTTT WHERE MAT_DOC BETWEEN '4186657603' AND '4186657604' ORDER BY MAT_DOC, MAT_ITEM"""
# query = """SELECT * FROM TTTT WHERE VALUE_LC = 'RUB'"""

spark.sql("DELETE FROM local.db.dwh_t_cmlc0991").show()
print(spark.sql("SELECT * FROM local.db.dwh_t_cmlc0991").count())
print(spark.sql("SELECT * FROM local.db.dwh_t_cmlc0992").count())
print(spark.sql("SELECT * FROM local.db.dwh_k_cmlc0991").count())
quit()
# spark.sql("SELECT * FROM local.db.dwh_k_cmlc0991 WHERE cnt > 1").show(10, truncate=False)
# df.printSchema() INTL
# spark.sql(query).show(100, truncate=False)
# spark.sql("SELECT mat_doc, doc_year, mat_item, stocktype FROM local.db.dwh_k_cmlc0991 MINUS SELECT mat_doc, doc_year, mat_item, stocktype FROM local.db.dwh_t_cmlc0992").show(20, truncate=False)
spark.sql("SELECT * FROM local.db.dwh_t_cmlc0991 WHERE mat_doc = '4186579824' AND mat_item = '0128' ORDER BY reqtsn, datapakid, record, recordmode, mat_doc, doc_year, mat_item, stocktype").show(20, truncate=False)
spark.sql("SELECT * FROM local.db.dwh_t_cmlc0992 WHERE mat_doc = '4186579824' AND mat_item = '0128' ORDER BY mat_doc, doc_year, mat_item, stocktype").show(20, truncate=False)
spark.sql("SELECT * FROM local.db.dwh_k_cmlc0991 WHERE mat_doc = '4186579824' AND mat_item = '0128'").show(20, truncate=False)
quit()
spark.sql("SELECT substr(mkey,1,23) as reqtsn, substr(mkey,24,6) as datapakid, cast(substr(mkey,30,8) as integer) as record, mkey FROM \
          (SELECT max(reqtsn || datapakid || record) as mkey FROM local.db.dwh_t_cmlc0991 WHERE mat_doc = '4186579824' AND mat_item = '0128') as q1").show(20, truncate=False)
# spark.sql("SELECT * FROM local.db.dwh_t_cmlc0991 WHERE mat_doc = '4186558380' ORDER BY reqtsn, datapakid, record, recordmode, mat_doc, doc_year, mat_item, stocktype").show(20, truncate=False)
# spark.sql("SELECT * FROM local.db.dwh_t_cmlc0992 WHERE mat_doc = '4186558380' ORDER BY mat_doc, doc_year, mat_item, stocktype").show(20, truncate=False)
# spark.sql("SELECT * FROM local.db.dwh_k_cmlc0991 WHERE mat_doc = '4186558380'").show(20, truncate=False)
query = """SELECT substr(mkey,1,23) as reqtsn
                        , substr(mkey,24,6) as datapakid
                        , cast(substr(mkey,30,8) as integer) as record
                        , mat_doc, doc_year, mat_item, stocktype 
                        FROM (SELECT max(reqtsn || datapakid || record) as mkey, mat_doc, doc_year, mat_item, stocktype FROM local.db.dwh_t_cmlc0991
                                GROUP BY mat_doc, doc_year, mat_item, stocktype
                                ORDER BY mat_doc, doc_year, mat_item, stocktype)"""
spark.sql(query).show(30, truncate=False)
quit()
# 20241220012631000086000
# 000007

# 4186649792 ZIBLNR = опять плохое, с запятыми

# query = '''SELECT material, matl_group || right(material,6) as material_ext, material_txt, base_uom, matl_group, matl_type, rpa_wgh1, rpa_wgh2, rpa_wgh3, rpa_wgh4 FROM {0}.dwh_t_material'''.format(sparkdb)
# query = '''SELECT calday, calday_d
#             FROM {0}.dwh_t_cmlc07p347 ORDER BY 1'''.format(sparkdb)


# query = '''DROP TABLE {0}.dwh_t_cmlc09922 PURGE'''.format(sparkdb)
icebergtbl_props = "icebergtbl_props.yaml"
request_fields = 'reqtsn STRING NOT NULL, datapakid STRING NOT NULL, record INT NOT NULL, recordmode STRING'
request_fields_insert = 'reqtsn, datapakid, record, recordmode'

with open(icebergtbl_props, "r") as f2:
    tbl_props_params = yaml.safe_load(f2)
tbl_props = tbl_props_params['iceberg_tbl_props_01']

query = """ CREATE OR REPLACE TABLE {2}.dwh_k_cmlc0991 ({0}, mat_doc STRING  NOT NULL, 
    doc_year STRING  NOT NULL, 
    mat_item STRING  NOT NULL, 
    stocktype STRING  NOT NULL, cnt INT NOT NULL ) USING iceberg TBLPROPERTIES {1}""".format(request_fields, 
        tbl_props, sparkdb)
spark.sql(query).show(30, truncate=False)
query = """INSERT INTO {0}.dwh_k_cmlc0991 (
            SELECT max(reqtsn), max(datapakid), max(record), max(recordmode),
            mat_doc, doc_year, mat_item, stocktype, count(*) as cnt FROM {0}.dwh_t_cmlc0991
            GROUP BY mat_doc, doc_year, mat_item, stocktype
            ORDER BY mat_doc, doc_year, mat_item, stocktype)""".format(sparkdb)


# # query = '''SELECT * FROM {0}.dwh_t_cmlc07p347.partitions'''.format(sparkdb)
# # query = '''SELECT * FROM {0}.dwh_t_cmlc07p347.manifests'''.format(sparkdb)
# # query = '''SELECT file_path, partition, record_count, file_size_in_bytes, column_sizes FROM {0}.dwh_t_cmlc07p347.files'''.format(sparkdb)
# query = "CREATE VIEW IF NOT EXISTS {0}.{1} AS (SELECT * FROM {0}.dwh_t_cmlc0992)".format(sparkdb, "dwh_v_cmlc09927")
spark.sql(query).show(30, truncate=False)

spark.stop()
