# geoCore_to_parquet
An AWS Lambda, built using AWS SAM, which converts multiple JSON files hosted on AWS S3 to a single parquet file.

This program can be triggered when a new JSON record is detected.

It is hoped that this program will improve the performance of queries, i.e., searching by keywords and by uuid. 

# Deployment as an image procedure using AWS SAM

```
cd geocore_to_parquet
sam build
sam local invoke
sam deploy --guided
```


# Deployment as a zip procedure

Assuming a CloudFormation template is created externally..

```
cd geocore_to_parquet
pip install -t python/ -r requirements.txt
cd python
zip -r geocore-to-parquet.zip ../app.py ../__init.py__ ./*
```
