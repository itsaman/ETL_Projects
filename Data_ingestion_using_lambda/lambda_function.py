import os
import requests
from upload import upload_s3

def lambda_handler(event, context):
    res = requests.get('https://data.gharchive.org/2021-01-29-0.json.gz')
    
    file = '2021-01-29-0.json.gz'
    buck_name = os.environ.get('BUCKET_NAME')
    file_prefix = os.environ.get('FILE_PREFIX')
    baseline_file = os.environ.get('BASELINE_FILE')
    body = res.content
    
    upload_res = upload_s3(buck_name, f'{file_prefix}/{file}', body)

    return upload_res