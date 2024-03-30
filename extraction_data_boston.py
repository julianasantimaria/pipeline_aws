import urllib.request
import pandas as pd
from decouple import config
from s3 import upload_to_s3
from io import BytesIO

bucket_name = "alura-datalake-juliana"

def extract_data(url, filename):
  try:
    urllib.request.urlretrieve(url, filename)

  except Exception as e:
    print(e)

extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/c9509ab4-6f6d-4b97-979a-0cf2a10c922b/download/tmphrybkxuh.csv", "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2015.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/b7ea6b1b-3ca4-4c5b-9713-6dc1db52379a/download/tmpzxzxeqfb.csv", "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2016.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/30022137-709d-465e-baae-ca155b51927d/download/tmpzccn8u4q.csv", "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2017.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/2be28d90-3a90-4af1-a3f6-f28c1e25880a/download/tmp7602cia8.csv", "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2018.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/ea2e4696-4a2d-429c-9807-d02eb92e0222/download/tmpcje3ep_w.csv", "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2019.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/6ff6a6fd-3141-4440-a880-6f60a37fe789/download/tmpcv_10m2s.csv", "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2020.csv")

arquivos = [
    "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2015.csv",
    "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2016.csv",
    "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2017.csv",
    "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2018.csv",
    "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2019.csv",
    "/Users/julianasantimaria/Desktop/data_pipeline_aws/data_boston/data_2020.csv",
]

dfs = {}

for arquivo in arquivos:
  ano = arquivo.split("_")[-1].split(".")[0]
  dfs[ano] = pd.read_csv(arquivo)

dfs["2018"].head()

#Conectando na conta de armazenamento

aws_access_key_id = config("aws_access_key_id")
aws_secret_access_key = config("aws_secret_access_key")
region_name = config("region_name")

boto3.setup_default_session(
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    region_name = region_name,
)

s3 = boto3.client("s3")

content = """
Olá, S3
"""

with open("hello-s3.txt", "w+") as f:
  f.write(content)

  s3.upload_file("hello-s3.txt", bucket_name, "bronze/hello-s3")

  #salvar arquivo em parquet

for ano, df in dfs.items():
  parquet_buffer = BytesIO()
  df.to_parquet(parquet_buffer)

  s3.put_object(
    Bucket=bucket_name,
    Key=f"bronze/dados_{ano}.parquet",
    Body=parquet_buffer.getvalue(),
  )

  response = s3.list_objects(Bucket=bucket_name)

  keys = [obj["Key"] for obj in response ["Contents"]]
  print(keys)