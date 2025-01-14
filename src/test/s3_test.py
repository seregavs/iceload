import boto3
import json
import os


YC_ACCESS_KEY_ID = os.environ.get("YC_ACCESS_KEY_ID") if os.environ.get("YC_ACCESS_KEY_ID") else ''
YC_SECRET_ACCESS_KEY = os.environ.get("YC_SECRET_ACCESS_KEY") if os.environ.get("YC_SECRET_ACCESS_KEY") else ''
YC_REGION_NAME = os.environ.get("YC_REGION_NAME") if os.environ.get("YC_REGION_NAME") else ''
YC_ENDPOINT_URL = os.environ.get("YC_ENDPOINT_URL") if os.environ.get("YC_ENDPOINT_URL") else ''

print('envs = ', YC_ACCESS_KEY_ID, YC_SECRET_ACCESS_KEY, YC_REGION_NAME, YC_ENDPOINT_URL)

session = boto3.session.Session(
    aws_access_key_id=YC_ACCESS_KEY_ID,
    aws_secret_access_key=YC_SECRET_ACCESS_KEY,
    region_name=YC_REGION_NAME)

s3 = session.client(
    service_name='s3',
    endpoint_url=YC_ENDPOINT_URL
)

#  for key in s3.list_objects(Bucket='stg-bi-1')['Contents']:
#     print(key['Key'])

res = s3.list_objects_v2(Bucket='stg-bi-1', Prefix='cmlc07p327', MaxKeys=2)
resource_list = list()
print(json.dumps(res, indent=2, default=str))
for item in res['Contents']:
    print(item['Key'])
    s = str(item['Key']).partition('/')[2]
    resource_list.append(s)

print(resource_list)
