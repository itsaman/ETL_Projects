import pandas as pd
import boto3
import io
from io import StringIO

def lambda_handler(event, context):
    s3_file_key = event['Records'][0]['s3']['object']['key'];
    bucket = 'sourcefile2001';
    s3 = boto3.client('s3', aws_access_key_id='AKIAQKNRPXXTBSVKSXA4',  aws_secret_access_key='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')
    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
    initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()));

    service_name = 's3'
    region_name = 'ap-south-1'
    aws_access_key_id = 'AKIAQKNRPXXTBSVKSXA4'
    aws_secret_access_key = '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'

    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    bucket='destinationfile2001';
    df = initial_df[(initial_df.species == "Iris-setosa")];
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False);
    s3_resource.Object(bucket, s3_file_key).put(Body=csv_buffer.getvalue())