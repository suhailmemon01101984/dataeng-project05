#####the below script was auto generated by glue visual etl. what i had to do is do a data source as aws glue data catalog
# then transform as change schema and then target as s3 bucket
# under the source aws glue catalog, i selected the suhailmemon84-dev.raw_statistics_data table
# under transform i changed the datatype for the columns: categoryid, views, likes, dislikes, comment count from string to bigint
# finally on the target s3 bucket, i made the format as parquet, compression snappy, s3 target location as: s3://suhailmemon84-youtube-de-project-cleaned/youtube/cleaned_stats_data/
# also chose the option: "create a table in the data catalog and on subsequent runs...keep existing schema...."
# chose database as suhailmemon84-dev and table as: cleaned_stats_data
#just a new comment
# then saved the visual etl job and then edited the script to add the code: push_down_predicate="region in('ca','gb','us')" on line 26. it wouldn't let me add this filter in visual etl mode so had to modify the script directly

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
AWSGlueDataCatalog_node1730254380787 = glueContext.create_dynamic_frame.from_catalog(database="suhailmemon84-dev", table_name="raw_statistics_data", transformation_ctx="AWSGlueDataCatalog_node1730254380787", push_down_predicate="region in('ca','gb','us')")

# Script generated for node Change Schema
ChangeSchema_node1730253570982 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1730254380787, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "bigint"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "bigint"), ("likes", "long", "likes", "bigint"), ("dislikes", "long", "dislikes", "bigint"), ("comment_count", "long", "comment_count", "bigint"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "string"), ("ratings_disabled", "boolean", "ratings_disabled", "string"), ("video_error_or_removed", "boolean", "video_error_or_removed", "string"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1730253570982")

# Script generated for node Amazon S3
AmazonS3_node1730253572773 = glueContext.getSink(path="s3://suhailmemon84-youtube-de-project-cleaned/youtube/cleaned_stats_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["region"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1730253572773")
AmazonS3_node1730253572773.setCatalogInfo(catalogDatabase="suhailmemon84-dev",catalogTableName="cleaned_stats_data")
AmazonS3_node1730253572773.setFormat("glueparquet", compression="snappy")
AmazonS3_node1730253572773.writeFrame(ChangeSchema_node1730253570982)
job.commit()

# when i ran the above job, it failed....because it couldn't parse the csv files which contained non english characters like japanese / chinese characters.
# so that's the reason why i added the code: push_down_predicate="region in('ca','gb','us')" in line 26 to make sure that aws only processes the canada, great britain and us files only because those files do not contain any special characters
# once that code was added, the subsequent run succeeded. you can verify by seeing the parquet files at: s3://suhailmemon84-youtube-de-project-cleaned/youtube/cleaned_stats_data/
# as well as the new catalog table that got created: "suhailmemon84-dev"."cleaned_stats_data". you can run a select over this table in athena to verify you get data back.
# i also added: partitionKeys=["region"] on line 32 where i'm configuring the sink to make sure my target glue table is partitioned on region
