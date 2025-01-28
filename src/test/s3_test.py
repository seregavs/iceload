import boto3
import json
import yaml
import os


# s3path = f'{self.metadata.rpartition('/')[0]}/{logfile}'
# pattern = r"s3a://([^/]+)/(.+)"
# match = re.match(pattern, s3path)
# if match:
#     bucket, s3key = match.group(1), match.group(2)

YC_ACCESS_KEY_ID = os.environ.get("YC_ACCESS_KEY_ID") if os.environ.get("YC_ACCESS_KEY_ID") else ''
YC_SECRET_ACCESS_KEY = os.environ.get("YC_SECRET_ACCESS_KEY") if os.environ.get("YC_SECRET_ACCESS_KEY") else ''
YC_REGION_NAME = os.environ.get("YC_REGION_NAME") if os.environ.get("YC_REGION_NAME") else ''
YC_ENDPOINT_URL = os.environ.get("YC_ENDPOINT_URL") if os.environ.get("YC_ENDPOINT_URL") else ''

# print('envs = ', YC_ACCESS_KEY_ID, YC_SECRET_ACCESS_KEY, YC_REGION_NAME, YC_ENDPOINT_URL)

session = boto3.session.Session(
    aws_access_key_id=YC_ACCESS_KEY_ID,
    aws_secret_access_key=YC_SECRET_ACCESS_KEY,
    region_name=YC_REGION_NAME)

s3 = session.client(
    service_name='s3',
    endpoint_url=YC_ENDPOINT_URL
)
# /

print('start reading S3')
response = s3.get_object(Bucket="stg-bi-1", Key="0datasource/bwp/settings_full.json")
ds_settings = json.loads(response['Body'].read().decode('utf-8'))
print(ds_settings)
record = next(item for item in ds_settings["cmlc01c"] if item["id"] == "1")
print(record["query"])
quit()
#  for key in s3.list_objects(Bucket='stg-bi-1')['Contents']:
#     print(key['Key'])

# res = s3.list_objects_v2(Bucket='stg-bi-1', Prefix='cmlc07p327', MaxKeys=2)
res = s3.list_objects_v2(Bucket='stg-bi-1', Prefix='0datasource/bwp/plant', MaxKeys=2)

resource_list = list()
# print(json.dumps(res, indent=2, default=str))
for item in res['Contents']:
    s = item['Key']
    # print(item['Key'])
    # s = str(item['Key']).partition('/')[2]
    if s[-1] != '/':
        resource_list.append(s)

# print(resource_list)
# for item in resource_list:
#     res = s3.get_object(Bucket='stg-bi-1', Key=item)
#     md_params = yaml.safe_load(res['Body'].read().decode('utf-8'))
#     print(md_params['plant']['srcfiles'])

# try:
#     res = s3.get_object(Bucket='stg-bi-1', Key='srcfile.log')
# except Exception as e:
#     print(e)
#     srcfiles_tmp = []
# else:
#     content2 = res['Body'].read().decode('utf-8')
#     srcfiles_tmp = str(content2).split('\n')
#     print(srcfiles_tmp)
# finally:
#     srcfiles = srcfiles_tmp
#     # srcfiles.append(srcfiles_tmp)
#     srcfiles.append('/home/alpine/iceload/data/cmlc01c/t1/cmlc01c1_20250110.orc')
#     srcfiles.append('/home/alpine/iceload/data/cmlc01c/t1/cmlc01c1_20250111.orc')
#     print(srcfiles)
#     s3body = '\n'.join(srcfiles)
#     print(s3body)
#     s3.put_object(Bucket='stg-bi-1', Key='srcfile.log', Body=s3body)
