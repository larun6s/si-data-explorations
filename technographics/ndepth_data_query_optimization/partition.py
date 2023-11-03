from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("Arun-NDepth-Bi-weekly-partition").getOrCreate()
df = spark.read.parquet("s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/")
df = df.withColumn("cid_hash", expr("abs(hash(company_gid)) % 50"))
output_path = "s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50"
df = df.write.partitionBy("cid_hash").save(output_path)
spark.stop()

# UI -> http://rm2-spark.prod1.6si.com:8088/cluster/app/application_1694772359728_486545 -> falied one



spark = SparkSession.builder.appName("Arun-NDepth-Bi-weekly-partition").getOrCreate()
df = spark.read.parquet("s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/")
df = df.withColumn("cid_hash", expr("abs(hash(company_gid)) % 50"))

# Function to monitor data skew and determine repartitioning
def check_and_repartition(df, partition_column):
    # Compute the current number of partitions
    num_partitions = df.rdd.getNumPartitions()

    # Perform monitoring (you can adjust this logic based on your data skew criteria)
    partition_sizes = df.groupBy(partition_column).count().rdd.map(lambda x: (x[0], x[1])).collect()
    avg_partition_size = df.count() / num_partitions
    skew_threshold = 1.5  # Adjust this threshold based on your data

    # If any partition size is significantly larger than the average, repartition
    partitions_to_repartition = [cid for cid, count in partition_sizes if count > skew_threshold * avg_partition_size]

    if partitions_to_repartition:
        # Calculate the new number of partitions
        new_num_partitions = num_partitions + len(partitions_to_repartition)

        # Repartition the DataFrame
        df = df.repartition(new_num_partitions, partition_column)

    return df

# Monitor data skew and repartition the DataFrame
df = check_and_repartition(df, "cid_hash")

# Continue with your Spark operations on the repartitioned DataFrame
output_path = "s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50"
df.write.partitionBy("cid_hash").save(output_path)
spark.stop()

# # UI -> http://rm2-spark.prod1.6si.com:8088/cluster/app/application_1694772359728_486568
# UI http://rm2-spark.prod1.6si.com:8088/proxy/application_1694772359728_486568/



spark = SparkSession.builder.appName("Arun-NDepth-Bi-weekly-partition").getOrCreate()
df = spark.read.parquet("s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/")
df = df.withColumn("cid_hash", expr("abs(hash(company_gid)) % 50"))
df = df.repartition(32768, "cid_hash")
output_path = "s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-4"
df.write.partitionBy("cid_hash").save(output_path)
spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
spark = SparkSession.builder.appName("Arun-NDepth-Bi-weekly-partition").getOrCreate()
ip_path = "s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/input/"
#ip_path = "s3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/"
df = spark.read.parquet(ip_path)
df = df.withColumn("cid_hash", expr("abs(hash(company_gid)) % 50"))
df = df.repartition(50,"cid_hash")
# Continue with your Spark operations on the repartitioned DataFrame
output_path = "s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8"
df.write.partitionBy("cid_hash").save(output_path)
spark.stop()