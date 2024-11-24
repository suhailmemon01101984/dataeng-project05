# went into aws glue and created a visual etl job. added 2 aws data catalog sources. one for the table: cleaned_stats_data and another for table:
# cleaned_stats_reference_data. then added a joiner block from the transforms section. made sure parents are the 2 aws data catalog sources i had
# changed join to inner join and added join condition as category id = id. added an aws s3 target block to create output in parquet and compression as snappy
# and chose the option to create a table in the data catalog and on subsequent runs keep existing schema and add new partitions. configured tablename as final_analytics for database: suhailmemon84-dev
# with partition keys as region for first level and category_id for 2nd level partitioning. saved the job and generated the script as below. ran the job and
# verified the bucket was there along with the files and i could query the final_analytics table from athena.
#
#

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1732487329060 = glueContext.create_dynamic_frame.from_catalog(database="suhailmemon84-dev", table_name="cleaned_stats_data", transformation_ctx="AWSGlueDataCatalog_node1732487329060")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1732487330415 = glueContext.create_dynamic_frame.from_catalog(database="suhailmemon84-dev", table_name="cleaned_stats_reference_data", transformation_ctx="AWSGlueDataCatalog_node1732487330415")

# Script generated for node Join
Join_node1732487364626 = Join.apply(frame1=AWSGlueDataCatalog_node1732487329060, frame2=AWSGlueDataCatalog_node1732487330415, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1732487364626")

# Script generated for node Amazon S3
AmazonS3_node1732487469919 = glueContext.getSink(path="s3://suhailmemon84-youtube-de-project-analytics/youtube/final_analytics/", connection_type="s3", updateBehavior="LOG", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732487469919")
AmazonS3_node1732487469919.setCatalogInfo(catalogDatabase="suhailmemon84-dev",catalogTableName="final_analytics")
AmazonS3_node1732487469919.setFormat("glueparquet", compression="snappy")
AmazonS3_node1732487469919.writeFrame(Join_node1732487364626)
job.commit()
