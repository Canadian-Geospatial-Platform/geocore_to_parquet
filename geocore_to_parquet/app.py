import os
import io
import json
import requests
import logging
import pandas as pd
import awswrangler as wr

from botocore.exceptions import ClientError

import boto3

GEOJSON_BUCKET_NAME = os.environ['GEOJSON_BUCKET_NAME']
PARQUET_BUCKET_NAME = os.environ['PARQUET_BUCKET_NAME']
def lambda_handler(event, context):
    """
    AWS Lambda Entry
    """
    #print(event)
    
    """PROD SETTINGS"""
    bucket_parquet = PARQUET_BUCKET_NAME
    region = "ca-central-1"
    s3_paginate_options = {'Bucket':GEOJSON_BUCKET_NAME} # Python dict, seperate with a comma: {'StartAfter'=2018,'Bucket'='demo'} see: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    s3_geocore_options = {'Bucket':GEOJSON_BUCKET_NAME}
    parquet_filename = "records.parquet"
    message = ""

    """ 
    Used for `sam local invoke -e payload.json` for local testing
    For actual use, comment out the two lines below 
    """
    
    #if "body" in event:
    #    event = json.loads(event["body"])
    
    """ 
    Parse query string parameters 
    """
    try:
        verbose = event["queryStringParameters"]["verbose"]
    except:
        verbose = False
        
    """
    Convert JSON files in the input bucket to parquet
    """
    
    #list all files in the s3 bucket
    try:
        filename_list = s3_filenames_paginated(region, **s3_paginate_options)
    except ClientError as e:
        print("Could not paginate the geojson bucket: %s" % e)
        
    #for each json file, open for reading, add to dataframe (df), close
    #note: if there are too many records to process, we may need to paginate 
    result = []
    count = 0
    for file in filename_list:
        #open the s3 file
        body = open_s3_file(file, **s3_geocore_options)
        #read the body
        json_body = json.loads(body)
        #append to the 'result' list
        result.append(json_body)
        count += 1
        #debug
        #if (count % 100) == 0:
        #    print(str(count))
        #    break
        
    try:
    	#normalize the geocore 'features' to pandas dataframe
        df = pd.json_normalize(result, 'features', record_prefix='features_')
        df.columns = df.columns.str.replace(r".", "_") #parquet does not support characters other than underscore
        
        #creates a new column called features_popularity and initializes to zero
        df['features_popularity'] = 0
        
        #todo detect if columb contains nested json format and do this transformation as a function
        df['features_properties_graphicOverview'] = df['features_properties_graphicOverview'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_contact'] = df['features_properties_contact'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_credits'] = df['features_properties_credits'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_cited'] = df['features_properties_cited'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_distributor'] = df['features_properties_distributor'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_options'] = df['features_properties_options'].apply(json.dumps, ensure_ascii=False)
        df = df.astype(pd.StringDtype()) #convert all columns to string
        
        #ID page currently expects "null" string instead of null type. Should be fixed on the javascript side next release
        df['features_properties_graphicOverview'] = df['features_properties_graphicOverview'].str.replace(': null',': "null"')
        df['features_properties_contact'] = df['features_properties_contact'].str.replace(': null',': "null"')
        df['features_properties_credits'] = df['features_properties_credits'].str.replace(': null',': "null"')
        df['features_properties_cited'] = df['features_properties_cited'].str.replace(': null',': "null"')
        df['features_properties_distributor'] = df['features_properties_distributor'].str.replace(': null',': "null"')
        df['features_properties_options'] = df['features_properties_options'].str.replace(': null',': "null"')
        
        #ID page needs lower case for onlineresource. Should be fixed on the javascript side next release
        df['features_properties_contact'] = df['features_properties_contact'].str.replace('onlineResource_Name', 'onlineresource_name')
        df['features_properties_contact'] = df['features_properties_contact'].str.replace('onlineResource_Protocol', 'onlineresource_protocol')
        df['features_properties_contact'] = df['features_properties_contact'].str.replace('onlineResource_Description', 'onlineresource_description')
        df['features_properties_contact'] = df['features_properties_contact'].str.replace('onlineResource', 'onlineresource')

    except:
        #too many things can go wrong
        message += "Some error occured normalizing the geojson record."
        print("Some error occured normalizing the geojson record.")
    
    """start debug block"""
    #print(count)
    #print(df.dtypes)
    #print(df.head())
    #temp_file = "records" + str(count) + ".json"
	#upload the appended json file to s3
    #upload_json_stream(temp_file, bucket_parquet, str(result))
    """end debug block"""
    
    #convert the appended json files to parquet format and upload to s3
    try:
        print("Trying to write to the S3 bucket: " + "s3://" + bucket_parquet + "/" + parquet_filename)
        wr.s3.to_parquet(
            df=df,
            path="s3://" + bucket_parquet + "/" + parquet_filename,
            dataset=False
        )
    except ClientError as e:
        print("Could not upload the parquet file: %s" % e)

    #clear result and dataframe
    result = []
    df = pd.DataFrame(None)
    
    if message == "":
        message += str(count) + " records have been inserted into the parquet file '" + parquet_filename + "' in " + bucket_parquet
        
    if verbose == "true" and len(filename_list) >0:
        message += '"uuid": ['
        for i in filename_list:
            #JSON format does not allow trailing commas for the last item of an array
            #See: https://www.json.org/json-en.html
            #Append comma if not the first element 
            if i:
                message += ","
            message += "{" + i + "}"
        message += "]"
			
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": message,
            }
        ),
    }
    
def s3_filenames_paginated(region, **kwargs):
    """Paginates a S3 bucket to obtain file names. Pagination is needed as S3 returns 999 objects per request (hard limitation)
    :param region: region of the s3 bucket 
    :param kwargs: Must have the bucket name. For other options see the list_objects_v2 paginator: 
    :              https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    :return: a list of filenames within the bucket
    """
    client = boto3.client('s3', region_name=region)
    
    paginator = client.get_paginator('list_objects_v2')
    result = paginator.paginate(**kwargs)
    
    filename_list = []
    count = 0
    
    for page in result:
        if "Contents" in page:
            for key in page[ "Contents" ]:
                keyString = key[ "Key" ]
                #print(keyString)
                count += 1
                filename_list.append(keyString)
    
    print("Bucket contains:", count, "files")
                
    return filename_list
    
def open_s3_file(filename, **kwargs):
    """Open a S3 file from bucket_name and filename and return the body as a string
    :param bucket_name: Bucket name
    :param filename: Specific file name to open
    :return: body of the file as a string
    """
    
    """
    Buffer to memory. Faster but memory intensive: https://stackoverflow.com/a/56814926
    """
    
    try:
        client = boto3.client('s3')
        bytes_buffer = io.BytesIO()
        client.download_fileobj(Key=filename, Fileobj=bytes_buffer, **kwargs)
        file_body = bytes_buffer.getvalue().decode() #python3, default decoding is utf-8
        #print (file_body)
        return str(file_body)
    except ClientError as e:
        logging.error(e)
        return False

def upload_json_stream(file_name, bucket, json_data, object_name=None):
    """Upload a json file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param json_data: stream of json data to write
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3 = boto3.resource('s3')
    try:
        s3object = s3.Object(bucket, file_name)
        response = s3object.put(Body=(bytes(json.dumps(json_data, indent=4, ensure_ascii=False).encode('utf-8'))))
    except ClientError as e:
        logging.error(e)
        return False
    return True
