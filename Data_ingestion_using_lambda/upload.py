import boto3

def get_client():
    return boto3.client('s3')

def upload_s3(buck_name, file, body):
    s3_client = get_client()
    res = s3_client.put_object(
        Bucket = buck_name, 
        Key = file,
        Body = body
    )
    return res