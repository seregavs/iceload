
import re

logfile = 'loaded.log'
s3path = 's3a://stg-bi-1/0datasource/bwp/plant/config.yaml'
s3path = f'{s3path.rpartition('/')[0]}/{logfile}'
print(s3path)

pattern = r"s3a://([^/]+)/(.+)"
match = re.match(pattern, s3path)
if match:
    bucket = match.group(1)
    s3key = match.group(2)
    print(bucket, '===', s3key)