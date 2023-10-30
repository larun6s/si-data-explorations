# /usr/lib/spark/bin/pyspark --executor-memory=20G --executor-cores=4 --driver-memory=19G --num-executors=15 --packages org.apache.spark:spark-avro_2.11:2.4.0
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("dapy")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.speculation", "false")
    .config("spark.sql.parquet.mergeSchema", "false")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.read.format("avro").load(
    "s3a://6si-dapy-1p-output/ndepth/crawl-output/bi-weekly/2023-10-17/"
)
df.write.mode("overwrite").parquet(
    "s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/"
)
# Above code succeeded in 40 mins - 128mb file size and snappy (compression) parquet format (columnar)
# Challenge - still data is not partitioned/bucketed on company_gid etc. so takes lot of time to query

df = spark.read.parquet(
    "s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/"
)
df.repartition(32768, ["company_gid"]).write.mode("overwrite").parquet(
    "s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet-partitioned/bi-weekly/2023-10-17/"
)
# Fails because of memory issues
