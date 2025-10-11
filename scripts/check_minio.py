import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:9002", aws_access_key_id="admin", aws_secret_access_key="admin123", config=boto3.session.Config(signature_version="s3v4"))
print(s3.list_buckets())
print("Objects in analytics:")
print(s3.list_objects_v2(Bucket="analytics"))
s3.download_file("analytics", "analytic_result_2025_10_05.csv", "outputs/analytic_result_2025_10_05_downloaded.csv")
print("Downloaded to outputs/")