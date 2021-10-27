# geoCore_to_parquet
An AWS Lambda, built using AWS SAM, which iterates through the geocore folder and appends each geojson file to a single parquet file (records.parquet). The parquet format is a columnar, binary data store (w/ snappy compression), meaning Athena SQL queries will only scan the specified columns of data instead of the entire JSON file. Additionally, having all of the JSON files in a single parquet file reduces the I/O overhead of Athena. Together these factors reduces time and money (amount of data scanned + number of S3 GET requests) by up to 90%.

A nice parquet viewer can be found here (.exe): https://github.com/mukunku/ParquetViewer/releases

This program can be triggered when a new JSON record is detected.

Creation of a lambda function called geocore-to-parquet which iterates through the geocore folder and appends each geojson file to a single parquet file . 

# Deployment as an image procedure using AWS SAM

```
cd geocore_to_parquet
sam build
sam local invoke
sam deploy --guided
```


# Deployment as a zip procedure

Zip deployment is complicated since the default python libraries exceed 250MB uncompressed size limit of a zipped AWS Lambda function. Therefore, it is needed to download the size-optimized AWS Wrangler Layer to bypass this issue.

First, go to:
```
https://github.com/awslabs/aws-data-wrangler/releases
```
and select the proper python version, i.e., for python 3.7, download: awswrangler-layer-2.12.1-py3.7.zip

Afterwards, copy botocore and boto3 libraries into the folder along with app.py. Cd into the folder and zip to geocore-to-parquet.zip

Assuming a CloudFormation template is created externally to extract the enviropment variables.

If size was not an issue, we could have run:

```
cd geocore_to_parquet
pip install -t python/ -r requirements.txt
cd python
zip -r geocore-to-parquet.zip ../app.py ../__init.py__ ./*
```
