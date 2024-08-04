import boto3
import os


def s3_upload_raw_stats_file(file, bucket):
    filename = f'./datafiles/{file}'
    bucket_file_name=f'youtube/raw_statistics_reference_data/{file}'
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket, bucket_file_name)

def s3_upload_region_data_file(file, bucket,region):
    filename = f'./datafiles/{file}'
    bucket_file_name=f'youtube/raw_statistics/region={region}/{file}'
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket, bucket_file_name)


bucketname = 'suhailmemon84-youtube-de-project'

directory = os.fsencode('./datafiles')
for file in os.listdir(directory):
    file_name = os.fsdecode(file)
    if file_name.endswith(".json"):
        s3_upload_raw_stats_file(file_name, bucketname)
    if file_name.endswith(".csv"):
        s3_upload_region_data_file(file_name,bucketname,file_name[:2].lower())

