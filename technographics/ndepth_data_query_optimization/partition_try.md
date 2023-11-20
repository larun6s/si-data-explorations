Created table through hive cli
```
CREATE EXTERNAL TABLE IF NOT EXISTS si_app_data_dev.arun_11gb_table
(
  crawl_timestamp BIGINT,
  company_id STRING,
  page_url STRING,
  final_url STRING,
  depth_level INT,
  page_source STRING,
  company_gid STRING,
  url_type STRING
)
PARTITIONED BY (cid_hash INT)
CLUSTERED BY (page_url) INTO 5 BUCKETS
STORED AS PARQUET
LOCATION 's3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/';

MSCK REPAIR TABLE si_app_data_dev.arun_11gb_table;
```

 Copied the data from above table to ss3 to check the structure. however, it downloaded many files each of them is parquet file
```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SaveToS3").getOrCreate()

s3_location = "s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/bucketed/"
your_hive_table_name = "si_app_data_dev.arun_11gb_table"
hive_table_data = spark.table(your_hive_table_name)


# Write the data to S3 in Parquet format
hive_table_data.write.mode("overwrite").parquet(s3_location)

spark.stop()
```

Understanding the table details
```
DESCRIBE FORMATTED si_app_data_dev.arun_11gb_table;
OK
# col_name            	data_type           	comment

crawl_timestamp     	bigint
company_id          	string
page_url            	string
final_url           	string
depth_level         	int
page_source         	string
company_gid         	string
url_type            	string

# Partition Information
# col_name            	data_type           	comment

cid_hash            	int

# Detailed Table Information
Database:           	si_app_data_dev
Owner:              	arun_lingala
CreateTime:         	Fri Nov 03 17:51:56 UTC 2023
LastAccessTime:     	UNKNOWN
Retention:          	0
Location:           	s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8
Table Type:         	EXTERNAL_TABLE
Table Parameters:
	EXTERNAL            	TRUE
	spark.sql.create.version	2.2 or prior
	spark.sql.sources.schema.bucketCol.0	page_url
	spark.sql.sources.schema.numBucketCols	1
	spark.sql.sources.schema.numBuckets	5
	spark.sql.sources.schema.numPartCols	1
	spark.sql.sources.schema.numParts	1
	spark.sql.sources.schema.part.0	{\"type\":\"struct\",\"fields\":[{\"name\":\"crawl_timestamp\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"company_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"page_url\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"final_url\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"depth_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"page_source\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"company_gid\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"url_type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cid_hash\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}
	spark.sql.sources.schema.partCol.0	cid_hash
	transient_lastDdlTime	1699034371

# Storage Information
SerDe Library:      	org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
InputFormat:        	org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
OutputFormat:       	org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
Compressed:         	No
Num Buckets:        	5
Bucket Columns:     	[page_url]
Sort Columns:       	[]
Storage Desc Params:
	serialization.format	1
Time taken: 0.292 seconds, Fetched: 46 row(s)
hive (default)>


ANALYZE TABLE si_app_data_dev.arun_11gb_table PARTITION(cid_hash) COMPUTE STATISTICS;
Query ID = arun_lingala_20231103181759_7aa4e4ad-f719-434a-ac71-cb8ab65d363a
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1694772359728_511550)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED    162        162        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 73.42 s
----------------------------------------------------------------------------------------------
Status: DAG finished successfully in 73.42 seconds


METHOD                         DURATION(ms)
parse                                    0
semanticAnalyze                          0
TezBuildDag                              0
TezSubmitToRunningDag                  247
TotalPrepTime                  1,699,035,487,936

VERTICES         TOTAL_TASKS  FAILED_ATTEMPTS KILLED_TASKS DURATION_SECONDS    CPU_TIME_MILLIS     GC_TIME_MILLIS  INPUT_RECORDS   OUTPUT_RECORDS
Map 1                    162                0            0            59.76          2,917,920            184,033        280,845                0
Partition si_app_data_dev.arun_11gb_table{cid_hash=0} stats: [numFiles=1, numRows=5734, totalSize=325229018, rawDataSize=51606]
Partition si_app_data_dev.arun_11gb_table{cid_hash=1} stats: [numFiles=1, numRows=5391, totalSize=249143117, rawDataSize=48519]
Partition si_app_data_dev.arun_11gb_table{cid_hash=10} stats: [numFiles=1, numRows=5632, totalSize=264455059, rawDataSize=50688]
Partition si_app_data_dev.arun_11gb_table{cid_hash=11} stats: [numFiles=1, numRows=5227, totalSize=188024263, rawDataSize=47043]
Partition si_app_data_dev.arun_11gb_table{cid_hash=12} stats: [numFiles=1, numRows=5102, totalSize=171664034, rawDataSize=45918]
Partition si_app_data_dev.arun_11gb_table{cid_hash=13} stats: [numFiles=1, numRows=5754, totalSize=252165544, rawDataSize=51786]
Partition si_app_data_dev.arun_11gb_table{cid_hash=14} stats: [numFiles=1, numRows=6109, totalSize=234375038, rawDataSize=54981]
Partition si_app_data_dev.arun_11gb_table{cid_hash=15} stats: [numFiles=1, numRows=5687, totalSize=259319999, rawDataSize=51183]
Partition si_app_data_dev.arun_11gb_table{cid_hash=16} stats: [numFiles=1, numRows=5675, totalSize=240903558, rawDataSize=51075]
Partition si_app_data_dev.arun_11gb_table{cid_hash=17} stats: [numFiles=1, numRows=5564, totalSize=264111675, rawDataSize=50076]
Partition si_app_data_dev.arun_11gb_table{cid_hash=18} stats: [numFiles=1, numRows=5798, totalSize=262252644, rawDataSize=52182]
Partition si_app_data_dev.arun_11gb_table{cid_hash=19} stats: [numFiles=1, numRows=5979, totalSize=262553607, rawDataSize=53811]
Partition si_app_data_dev.arun_11gb_table{cid_hash=2} stats: [numFiles=1, numRows=5833, totalSize=285949553, rawDataSize=52497]
Partition si_app_data_dev.arun_11gb_table{cid_hash=20} stats: [numFiles=1, numRows=4935, totalSize=211018541, rawDataSize=44415]
Partition si_app_data_dev.arun_11gb_table{cid_hash=21} stats: [numFiles=1, numRows=5233, totalSize=238290841, rawDataSize=47097]
Partition si_app_data_dev.arun_11gb_table{cid_hash=22} stats: [numFiles=1, numRows=5843, totalSize=296059106, rawDataSize=52587]
Partition si_app_data_dev.arun_11gb_table{cid_hash=23} stats: [numFiles=1, numRows=4988, totalSize=260156323, rawDataSize=44892]
Partition si_app_data_dev.arun_11gb_table{cid_hash=24} stats: [numFiles=1, numRows=5712, totalSize=226043219, rawDataSize=51408]
Partition si_app_data_dev.arun_11gb_table{cid_hash=25} stats: [numFiles=1, numRows=6056, totalSize=263892715, rawDataSize=54504]
Partition si_app_data_dev.arun_11gb_table{cid_hash=26} stats: [numFiles=1, numRows=6487, totalSize=326273289, rawDataSize=58383]
Partition si_app_data_dev.arun_11gb_table{cid_hash=27} stats: [numFiles=1, numRows=5458, totalSize=226329523, rawDataSize=49122]
Partition si_app_data_dev.arun_11gb_table{cid_hash=28} stats: [numFiles=1, numRows=6144, totalSize=261700007, rawDataSize=55296]
Partition si_app_data_dev.arun_11gb_table{cid_hash=29} stats: [numFiles=1, numRows=5650, totalSize=267244503, rawDataSize=50850]
Partition si_app_data_dev.arun_11gb_table{cid_hash=3} stats: [numFiles=1, numRows=4512, totalSize=193306736, rawDataSize=40608]
Partition si_app_data_dev.arun_11gb_table{cid_hash=30} stats: [numFiles=1, numRows=5569, totalSize=257135108, rawDataSize=50121]
Partition si_app_data_dev.arun_11gb_table{cid_hash=31} stats: [numFiles=1, numRows=6310, totalSize=275702182, rawDataSize=56790]
Partition si_app_data_dev.arun_11gb_table{cid_hash=32} stats: [numFiles=1, numRows=5166, totalSize=218247964, rawDataSize=46494]
Partition si_app_data_dev.arun_11gb_table{cid_hash=33} stats: [numFiles=1, numRows=5924, totalSize=259774637, rawDataSize=53316]
Partition si_app_data_dev.arun_11gb_table{cid_hash=34} stats: [numFiles=1, numRows=5135, totalSize=224646167, rawDataSize=46215]
Partition si_app_data_dev.arun_11gb_table{cid_hash=35} stats: [numFiles=1, numRows=6321, totalSize=291245999, rawDataSize=56889]
Partition si_app_data_dev.arun_11gb_table{cid_hash=36} stats: [numFiles=1, numRows=5851, totalSize=255293777, rawDataSize=52659]
Partition si_app_data_dev.arun_11gb_table{cid_hash=37} stats: [numFiles=1, numRows=5395, totalSize=270008016, rawDataSize=48555]
Partition si_app_data_dev.arun_11gb_table{cid_hash=38} stats: [numFiles=1, numRows=5371, totalSize=288837182, rawDataSize=48339]
Partition si_app_data_dev.arun_11gb_table{cid_hash=39} stats: [numFiles=1, numRows=6095, totalSize=351045054, rawDataSize=54855]
Partition si_app_data_dev.arun_11gb_table{cid_hash=4} stats: [numFiles=1, numRows=5829, totalSize=231122847, rawDataSize=52461]
Partition si_app_data_dev.arun_11gb_table{cid_hash=40} stats: [numFiles=1, numRows=5805, totalSize=257883077, rawDataSize=52245]
Partition si_app_data_dev.arun_11gb_table{cid_hash=41} stats: [numFiles=1, numRows=5349, totalSize=226116238, rawDataSize=48141]
Partition si_app_data_dev.arun_11gb_table{cid_hash=42} stats: [numFiles=1, numRows=5202, totalSize=226495829, rawDataSize=46818]
Partition si_app_data_dev.arun_11gb_table{cid_hash=43} stats: [numFiles=1, numRows=6265, totalSize=276636559, rawDataSize=56385]
Partition si_app_data_dev.arun_11gb_table{cid_hash=44} stats: [numFiles=1, numRows=6024, totalSize=302010713, rawDataSize=54216]
Partition si_app_data_dev.arun_11gb_table{cid_hash=45} stats: [numFiles=1, numRows=5523, totalSize=228604629, rawDataSize=49707]
Partition si_app_data_dev.arun_11gb_table{cid_hash=46} stats: [numFiles=1, numRows=5585, totalSize=268667840, rawDataSize=50265]
Partition si_app_data_dev.arun_11gb_table{cid_hash=47} stats: [numFiles=1, numRows=5080, totalSize=231257452, rawDataSize=45720]
Partition si_app_data_dev.arun_11gb_table{cid_hash=48} stats: [numFiles=1, numRows=6439, totalSize=295722251, rawDataSize=57951]
Partition si_app_data_dev.arun_11gb_table{cid_hash=49} stats: [numFiles=1, numRows=4368, totalSize=205750890, rawDataSize=39312]
Partition si_app_data_dev.arun_11gb_table{cid_hash=5} stats: [numFiles=1, numRows=5172, totalSize=229752781, rawDataSize=46548]
Partition si_app_data_dev.arun_11gb_table{cid_hash=6} stats: [numFiles=1, numRows=5631, totalSize=271826835, rawDataSize=50679]
Partition si_app_data_dev.arun_11gb_table{cid_hash=7} stats: [numFiles=1, numRows=6013, totalSize=250929146, rawDataSize=54117]
Partition si_app_data_dev.arun_11gb_table{cid_hash=8} stats: [numFiles=1, numRows=5664, totalSize=242679420, rawDataSize=50976]
Partition si_app_data_dev.arun_11gb_table{cid_hash=9} stats: [numFiles=1, numRows=5256, totalSize=249581712, rawDataSize=47304]
OK
Time taken: 115.235 seconds
```

To understand how much time it takes in worst case
```
EXPLAIN FORMATTED SELECT * FROM si_app_data_dev.arun_11gb_table  WHERE page_url="123";



EXPLAIN FORMATTED SELECT * FROM si_app_data_dev.arun_11gb_table  WHERE page_url="123";
OK
{"STAGE DEPENDENCIES":{"Stage-1":{"ROOT STAGE":"TRUE"},"Stage-0":{"DEPENDENT STAGES":"Stage-1"}},"STAGE PLANS":{"Stage-1":{"Tez":{"DagId:":"arun_lingala_20231103182140_96e56c9b-6b16-49bf-8866-3eb97438cd55:2","DagName:":"","Vertices:":{"Map 1":{"Map Operator Tree:":[{"TableScan":{"alias:":"arun_11gb_table","Statistics:":"Num rows: 280845 Data size: 2527605 Basic stats: COMPLETE Column stats: PARTIAL","children":{"Filter Operator":{"predicate:":"(page_url = '123') (type: boolean)","Statistics:":"Num rows: 140422 Data size: 561688 Basic stats: COMPLETE Column stats: PARTIAL","children":{"Select Operator":{"expressions:":"crawl_timestamp (type: bigint), company_id (type: string), '123' (type: string), final_url (type: string), depth_level (type: int), page_source (type: string), company_gid (type: string), url_type (type: string), cid_hash (type: int)","outputColumnNames:":["_col0","_col1","_col2","_col3","_col4","_col5","_col6","_col7","_col8"],"Statistics:":"Num rows: 140422 Data size: 12778402 Basic stats: COMPLETE Column stats: PARTIAL","children":{"File Output Operator":{"compressed:":"true","Statistics:":"Num rows: 140422 Data size: 12778402 Basic stats: COMPLETE Column stats: PARTIAL","table:":{"input format:":"org.apache.hadoop.mapred.TextInputFormat","output format:":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat","serde:":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}}}}}}}}}]}}}},"Stage-0":{"Fetch Operator":{"limit:":"-1","Processor Tree:":{"ListSink":{}}}}}}
Time taken: 2.274 seconds, Fetched: 1 row(s)

INSERT OVERWRITE DIRECTORY 's3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/hive_bucket' SELECT * FROM si_app_data_dev.arun_11gb_table;

INSERT OVERWRITE DIRECTORY 's3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/hive_bucket' SELECT * FROM si_app_data_dev.arun_11gb_table;
Query ID = arun_lingala_20231103183142_72418c3b-b8d2-4bb7-bcf5-6fc3b9995b37
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1694772359728_511551)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED    162        162        0        0       0       2
----------------------------------------------------------------------------------------------
VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 118.65 s
----------------------------------------------------------------------------------------------
Status: DAG finished successfully in 118.65 seconds


METHOD                         DURATION(ms)
parse                                    0
semanticAnalyze                          0
TezBuildDag                              0
TezSubmitToRunningDag                  799
TotalPrepTime                  1,699,036,312,682

VERTICES         TOTAL_TASKS  FAILED_ATTEMPTS KILLED_TASKS DURATION_SECONDS    CPU_TIME_MILLIS     GC_TIME_MILLIS  INPUT_RECORDS   OUTPUT_RECORDS
Map 1                    162                0            2           106.14          5,922,800            340,312        280,845                0
Moving data to: s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/hive_bucket
OK
Time taken: 681.804 seconds
```

Write the data from table to s3 through hive cli
```
INSERT OVERWRITE DIRECTORY 's3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/hive_bucket' SELECT * FROM si_app_data_dev.arun_11gb_table;
Query ID = arun_lingala_20231103183142_72418c3b-b8d2-4bb7-bcf5-6fc3b9995b37
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.


Status: Running (Executing on YARN cluster with App id application_1694772359728_511551)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED    162        162        0        0       0       2
----------------------------------------------------------------------------------------------
VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 118.65 s
----------------------------------------------------------------------------------------------
Status: DAG finished successfully in 118.65 seconds


METHOD                         DURATION(ms)
parse                                    0
semanticAnalyze                          0
TezBuildDag                              0
TezSubmitToRunningDag                  799
TotalPrepTime                  1,699,036,312,682

VERTICES         TOTAL_TASKS  FAILED_ATTEMPTS KILLED_TASKS DURATION_SECONDS    CPU_TIME_MILLIS     GC_TIME_MILLIS  INPUT_RECORDS   OUTPUT_RECORDS
Map 1                    162                0            2           106.14          5,922,800            340,312        280,845                0
Moving data to: s3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/hive_bucket
OK
Time taken: 681.804 seconds
hive (default)>
```

Select latency
```
SELECT * FROM si_app_data_dev.arun_11gb_table  WHERE page_url="123";
Query ID = arun_lingala_20231103182216_c891a090-e424-4ab9-83e8-b109fcc4cd4e
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1694772359728_511550)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED    162        162        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 99.05 s
----------------------------------------------------------------------------------------------
Status: DAG finished successfully in 99.05 seconds


METHOD                         DURATION(ms)
parse                                    0
semanticAnalyze                          0
TezBuildDag                              0
TezSubmitToRunningDag                  211
TotalPrepTime                  1,699,035,737,497

VERTICES         TOTAL_TASKS  FAILED_ATTEMPTS KILLED_TASKS DURATION_SECONDS    CPU_TIME_MILLIS     GC_TIME_MILLIS  INPUT_RECORDS   OUTPUT_RECORDS
Map 1                    162                0            0            87.18          3,691,490            556,731        280,845                0
OK
Time taken: 101.236 seconds
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS si_app_data_dev.arun_11gb_structure
(
  crawl_timestamp BIGINT,
  company_id STRING,
  page_url STRING,
  final_url STRING,
  depth_level INT,
  page_source STRING,
  company_gid STRING,
  url_type STRING
)
PARTITIONED BY (cid_hash INT)
CLUSTERED BY (page_url) INTO 5 BUCKETS
STORED AS PARQUET
LOCATION 's3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition-bucketed-final/';


INSERT INTO TABLE si_app_data_dev.arun_11gb_structure
SELECT *
FROM si_app_data_dev.arun_11gb_table;
```

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data from the source_table into the destination_table with dynamic partitioning.
-- This script will insert data into the destination table while determining the partition value from the source table.
INSERT OVERWRITE TABLE si_app_data_dev.arun_11gb_structure
PARTITION (cid_hash)
SELECT *, cid_hash AS destination_partition_value FROM si_app_data_dev.arun_11gb_table limit 2;


LOAD DATA INPATH 's3a://6si-customers-adhoc/test_lak/ndepth_partitioning/2023-10-31/output/partition50-8/cid_hash=0' INTO TABLE si_app_data_dev.arun_11gb_structure PARTITION (cid_hash=51);

SELECT numBuckets, bucketColumns FROM si_app_data_dev.arun_11gb_structure;


SHOW TABLE EXTENDED LIKE  si_app_data_dev.arun_11gb_structure;


We have 30TB of crawl data. Following is the schema
```
{
        "namespace": "com.sixsense.dapy.ndepth",
        "type": "record",
        "name": "PageRecord",
        "fields":
        [
        {"name": "crawl_timestamp", "type": "long"},
        {"name": "company_id", "type": "string"},
        {"name": "page_url", "type": "string"},
        {"name": "final_url", "type": "string"},
        {"name": "depth_level", "type": "int"},
        {"name": "page_source", "type": "string"},
        {"name": "company_gid", "type": "string"},
        {"name": "url_type", "type": "string"}
        ]
}
```
company_gis cardinality is 17M
page_url cardinality is 800M
depth cardinality is 3
we need to create external tables on them to efficiently query records pertaining to 
1. a page_url
2. a page_url and a company_gid
3. a company_gid and depth
4. a company_gid


So, we tried to partition the data by company_gid and bucket by page_url. However, encountered blockers continuously.
We tried the following
taken relatively smaller data i.e. 4TB with company_gid, page_url cardinality being 1M, 100M respectively. But, could not partition as aggregations failed with different memory related errors for multiple attempts.
however, we could succeeded with 11gb of data.

So, I am curious to understand whether 6si platforms has limitations w.r.t. data size, cardinality etc. 
