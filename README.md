# geoCore_to_parquet
An AWS Lambda, built using AWS SAM, which iterates through the geocore folder and appends each geojson file to a single parquet file (records.parquet). The parquet format is a columnar, binary data store (w/ snappy compression), meaning Athena SQL queries will only scan the specified columns of data instead of the entire JSON file. Additionally, having all of the JSON files in a single parquet file reduces the I/O overhead of Athena. Together these factors reduces time and money (amount of data scanned + number of S3 GET requests) by up to 90%.

A nice parquet viewer can be found here (warning `.exe`): https://github.com/mukunku/ParquetViewer/releases

This program can be triggered when a new JSON record is detected although is currently being run on an hourly time schedule.

Creation of a lambda function called geocore-to-parquet which iterates through the geocore folder and appends each geojson file to a single parquet file. 

# Deployment as an image using AWS SAM

```
cd geocore_to_parquet
sam build
sam local invoke
sam deploy --guided
```

# Deployment as a zip using size-optimized AWS Lambda layers

Zip deployment of this project is complicated since the default python libraries exceed the imposed 250MB uncompressed size limit of a zipped AWS Lambda function. Therefore, it is needed to download the size-optimized AWS Wrangler Layers to bypass this issue.

First, go to:
```
https://github.com/awslabs/aws-data-wrangler/releases
```
and select the proper python version. For example, for python 3.7, download: awswrangler-layer-2.12.1-py3.7.zip

Afterwards, copy botocore and boto3 libraries into the folder along with app.py. Note: botocore and boto3 come with the environment, but it is good practise to 'lock' these libraries for maximum compatability.

Change directory (cd) into the folder and use zip to create the zip package. I.e., `zip -r geocore-to-parquet.zip ../app.py ./*`

Here, I assume a CloudFormation template has been created externally to extract the enviropment variables.

# Deployment as a zip using pip

If size was not an issue, we can use `pip install` and target the `requirements.txt` to install the libraries inside of a new folder (say 'my_lambda') and run the following commands.

Note: the app.py and any source code must appear in the root of the zip package. 

For more information, refer to the [packaging documentation on AWS](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-create-package-with-dependency)

```
cd geocore_to_parquet
pip install -t my_lambda/ -r requirements.txt
cd my_lambda
zip -r geocore-to-parquet.zip ../app.py ../__init.py__ ./*
```
