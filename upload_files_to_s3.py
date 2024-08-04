import boto3
import os


def s3_upload_file(file, bucket):
    filename = f'./datafiles/{file}'
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket, file)


bucketname = 'suhailmemon84-youtube-de-project'

directory = os.fsencode('./datafiles')
for file in os.listdir(directory):
    file_name = os.fsdecode(file)
    s3_upload_file(file_name, bucketname)
