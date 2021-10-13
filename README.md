# geoCore_to_parquet
An AWS Lambda, built using AWS SAM, which converts multiple JSON files hosted on AWS S3 to a single parquet file.

This program can be triggered when a new JSON record is detected.

It is hoped that this program will improve the performance of queries, i.e., searching by keywords and by uuid. 
