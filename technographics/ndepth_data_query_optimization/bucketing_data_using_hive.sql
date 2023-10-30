-- Attempt 1
-- Step 1: Table over raw avro data
CREATE EXTERNAL TABLE IF NOT EXISTS si_app_data_dev.ndepth_biweekly_crawl
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION 's3a://6si-dapy-1p-output/ndepth/crawl-output/bi-weekly/2023-10-17/'
    TBLPROPERTIES (
     'avro.schema.literal'='{
       "namespace": "com.sixsense.dapy.ndepth",
       "name": "PageRecord",
       "type": "record",
       "fields": [ {"name": "crawl_timestamp", "type": "long"},
                    {"name": "company_id", "type": "string"},
                    {"name": "page_url", "type": "string"},
                    {"name": "final_url", "type": "string"},
                    {"name": "depth_level", "type": "int"},
                    {"name": "page_source", "type": "string"},
                    {"name": "company_gid", "type": "string"},
                    {"name": "url_type", "type": "string"} 
                ]
    }')

-- Step 2: empty hive bucketed Table
CREATE EXTERNAL TABLE IF NOT EXISTS navjot_db.ndepth_biweekly_crawl_bucketed(
    crawl_timestamp bigint,
	company_id string,
	page_url string,
	final_url string,
	depth_level int,
	page_source string,
	company_gid string,
	url_type string )
    CLUSTERED BY (company_gid) SORTED BY (depth_level) INTO 32768 BUCKETS
    STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 's3a://6si-customers-dev/test_navjot/dacq_output/ndepth/crawl-output-bucketed/bi-weekly/2023-10-17/';

-- Step 3: Insert data into bucketed hive table
INSERT OVERWRITE TABLE navjot_db.ndepth_biweekly_crawl_bucketed select * from si_app_data_dev.ndepth_biweekly_crawl;

-- Above attempt failed with OOM issues
-- http://nn1.prod1.6si.com/tez-ui/#/dag/dag_1697694181951_864651_1

-- Attempt 2
-- Step 1 - Parquet data from avro via Spark - 128mb file size
-- Step 2 - Hive table over parquet data created via Spark
CREATE EXTERNAL TABLE IF NOT EXISTS navjot_db.ndepth_biweekly_crawl_parquet(
    crawl_timestamp bigint,
	company_id string,
	page_url string,
	final_url string,
	depth_level int,
	page_source string,
	company_gid string,
	url_type string )
    STORED AS PARQUET
    LOCATION 's3a://6si-customers-adhoc/test_navjot/dacq_output/ndepth/crawl-output-parquet/bi-weekly/2023-10-17/';

-- Step 3 - Bucketing over parquet data hive table
INSERT OVERWRITE TABLE navjot_db.ndepth_biweekly_crawl_bucketed select
    crawl_timestamp,
	company_id,
	page_url,
	final_url,
	depth_level,
	page_source,
	company_gid,
	url_type
	from navjot_db.ndepth_biweekly_crawl_parquet

-- failed with OOM issue again