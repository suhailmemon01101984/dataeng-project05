import boto3


def bucket_exists(bucket):
    s3 = boto3.resource('s3')
    return s3.Bucket(bucket) in s3.buckets.all()


s3 = boto3.client('s3')

if bucket_exists('suhailmemon84-youtube-de-project'):
    print('bucket exists')
else:
    s3.create_bucket(Bucket='suhailmemon84-youtube-de-project')

response = s3.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets']]
print(buckets)
