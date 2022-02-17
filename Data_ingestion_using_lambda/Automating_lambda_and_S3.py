import boto3, os
import requests
from datetime import datetime as dt
from datetime import timedelta as td
from botocore.errorfactory import ClientError

baseline_file = '2021-01-29-0.json.gz'

#timedelta will add hours by 1 
#datetime converting string into date/time
#striptime will convert into date/time
#strftime to be used for building file name
# '%Y-%M-%d-%#H' here # is used to convert 2 digits into single digit.

for i in range(1,3):
    dt_part = baseline_file.split('.')[0]
    next_file = f"{dt.strftime(dt.strptime(dt_part, '%Y-%M-%d-%H') + td(hours = i), '%Y-%M-%d-%#H')}.json.gz"
    all_files = f"https://data.gharchive.org/{next_file}"
    res =  requests.get(all_files)
    
    print(f"the status code for {next_file} and the status code {res.status_code}")
    
 
#Creating bookmark for managing files in s3
s3_client = boto3.client('s3')

bookmark_contents = '2021-01-30-0.json.gz'

try:
    bookmark_file = s3_client.get_object(
        Bucket= 'github0602',
        Key = 'sandbox/bookmark'
    )
    print(bookmark_file['Body'].read().decode('utf-8'))

#Error code was NoSuchKey
except ClientError as e:
   pass
