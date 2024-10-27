#create the lambda function using the aws console UI. python 3.12 as runtime, x86_64 as architecture, change the default execution role to
#suhailmemon84-lambda-s3-glue-access.
#once the function is created, go under configuration, then general configuration then click edit and update memory and ephemeral storage both to 512 mb
#and update timeout to 5 min 3 seconds.
#finally go under the code tab, scroll down and then go in the section: Layers and click on add a layer and add the layer: aws layers --> awssdkpandas-python312
#details about the role: suhailmemon84-lambda-s3-glue-access. create this role under IAM and grant it amazons3fullaccess awsconsolefullaccess and awsglueservicerole

#plug below code under lambda_function.py and test with the s3 PUT test event JSON
# under bucket, change name as "name": "suhailmemon84-youtube-de-project"
# make arn as "arn": "arn:aws:s3:::suhailmemon84-youtube-de-project"
# make key under object as: "key": "youtube/raw_statistics_reference_data/CA_category_id.json",
# go under function configuration and add environment variables: env_clean_s3_path, env_glue_database, env_glue_table, env_glue_table_write_mode
# with values: s3://suhailmemon84-youtube-de-project-cleaned/youtube, suhailmemon84-dev, cleaned_stats_reference_data and append
#then deploy your function and click the test button

import json
import os
import boto3
import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    # TODO implement
    env_clean_s3_path=os.getenv('env_clean_s3_path')
    env_glue_database=os.getenv('env_glue_database')
    env_glue_table=os.getenv('env_glue_table')
    env_glue_table_write_mode=os.getenv('env_glue_table_write_mode')
    bucketname=event["Records"][0]["s3"]["bucket"]["name"]
    bucketfilename=event["Records"][0]["s3"]["object"]["key"]
    s3filepath=f's3://{bucketname}/{bucketfilename}'
    df_json = wr.s3.read_json(path=s3filepath)
    df_items=pd.json_normalize(df_json["items"])
    #print(df_json["items"])
    #print(df_items)
    #df_items=df_json["items"]
    #print(type(df_items))
    wr.s3.to_parquet(df=df_items, path=env_clean_s3_path, dataset=True, database=env_glue_database, table=env_glue_table, mode=env_glue_table_write_mode )


    return {
        'statusCode': 200,
        'body': json.dumps('Suhail, Hello from Lambda!')
    }

