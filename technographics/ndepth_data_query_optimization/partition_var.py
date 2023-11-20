from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
#ip="s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/"
ip="s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-7/"
op="s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/4TB/"
spark = SparkSession.builder.appName("longrunning_ndepth-4tb-partition").getOrCreate()
df = spark.read.parquet(ip)
df = df.withColumn("cid_hash", expr("abs(hash(company_gid)) % 200"))
df = df.repartition(200, "cid_hash")
df = df.write.partitionBy("cid_hash").save(op)
spark.stop()

# Try external shuffle
# set setting to save data to disk