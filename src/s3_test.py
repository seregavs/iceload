import boto3
import json

session = boto3.session.Session(
    aws_access_key_id=yc_access_key_id,
    aws_secret_access_key=yc_secret_access_key,
    region_name=yc_region_name)

s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
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
