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
DYNAMODB_TABLE      = os.environ['DYNAMODB_TABLE']
REGION_NAME         = os.environ['REGION_NAME']

def lambda_handler(event, context):
    """
    AWS Lambda Entry
    """
    #print(event)
    
    """PROD SETTINGS"""
    bucket_parquet = PARQUET_BUCKET_NAME
    s3_paginate_options = {'Bucket':GEOJSON_BUCKET_NAME} # Python dict, seperate with a comma: {'StartAfter'=2018,'Bucket'='demo'} see: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    s3_geocore_options = {'Bucket':GEOJSON_BUCKET_NAME}
    parquet_filename = "records.parquet"
    region = REGION_NAME
    message = ""
    log_level = ""

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
    if log_level == "DEBUG":
        print(pd.__version__) #print pandas version; version >1.3.0 is expected
        filename_list = filename_list[0:500] # DEBUG -- read first 50 geojson files
        for file in filename_list:
            print(file)
            

    for file in filename_list:
        #open the s3 file
        body = open_s3_file(file, **s3_geocore_options)
        #check if file is empty. if so, skip this iteration
        if body == "" or body == None:
            continue
        #read the body
        json_body = json.loads(body)
        #append to the 'result' list
        result.append(json_body)
        count += 1
        #debug
        if log_level == "DEBUG":
            if (count % 100) == 0:
                print(str(count))

    # Read all items in the popularity and similarity table from dynamodb
    # See scan method: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan
    popularity_df = dynamodb_table_to_df('analytics_popularity')
    similarity_df = dynamodb_table_to_df('similarity')
    
    # Rename the popularity table 
    popularity_df = popularity_df[['popularity', 'uuid']]
    popularity_df.rename(columns={'popularity':'features_popularity'}, inplace = True)
    popularity_df.rename(columns={'uuid':'features_properties_id'}, inplace = True)
    
    # Rename the similarity table 
    similarity_df = similarity_df[['similarity', 'features_properties_id']]
    similarity_df.rename(columns={'similarity':'features_similarity'}, inplace = True)
    
    df = pd.json_normalize(result, 'features', record_prefix='features_')
        
    try:
    	#normalize the geocore 'features' to pandas dataframe
        df.columns = df.columns.str.replace(r".", "_") #parquet does not support characters other than underscore
        
        #todo detect if column contains nested json format and do this transformation as a function
        df['features_properties_graphicOverview'] = df['features_properties_graphicOverview'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_contact'] = df['features_properties_contact'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_credits'] = df['features_properties_credits'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_cited'] = df['features_properties_cited'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_distributor'] = df['features_properties_distributor'].apply(json.dumps, ensure_ascii=False)
        df['features_properties_options'] = df['features_properties_options'].apply(json.dumps, ensure_ascii=False)
        try:
            df['features_properties_plugins'] = df['features_properties_plugins'].apply(json.dumps, ensure_ascii=False)
        except:
            message += "No plugins column"
            print("No plugins column")

        df = df.astype(pd.StringDtype()) #convert all columns to string
        df = df.replace({'NaN': '[]'})
        
        if log_level == "DEBUG":
            print(df.dtypes)
        
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
        
        #modifies dates to acceptable values
        df['features_properties_date_published_date'] = df['features_properties_date_published_date'].str.replace('Not Available; Indisponible', '2022-01-01')
        
        #drop existing popularity if it exists
        #if 'features_popularity' in df.columns:
        #    df = df.drop('features_popularity', 1)

    except:
        #too many things can go wrong
        message += "Some error occured normalizing the geojson record."
        print("Some error occured normalizing the geojson record.")

    #merge popularity_df with df based on uuid and then sort by popularity, replace NaN with 0 for popularity
    if log_level == "DEBUG":
        print("df size: ", df.shape[0])
        print("popularity_df size: ", popularity_df.shape[0])
    df_final = df.merge(popularity_df, on='features_properties_id', how='left')
    df_final = df_final.sort_values(by=['features_popularity'], ascending=False)
    df_final['features_popularity'].fillna(0, inplace=True)
    
    #merge similarity_df with df_final based on uuid
    df_final = df_final.merge(similarity_df, on='features_properties_id', how='left')
    
    if log_level == "DEBUG":
        print("df_final size: ", df_final.shape[0])
        na_summary = df.isna().sum()
        print(f'The Nas in the merged dataframe is {na_summary}')
        
    """start debug block"""
    #if log_level == "DEBUG":
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
            df=df_final,
            path="s3://" + bucket_parquet + "/" + parquet_filename,
            dataset=False
        )
    except ClientError as e:
        print("Could not upload the parquet file: %s" % e)

    #clear result and dataframe
    result = []
    df = pd.DataFrame(None)
    
    message += " " + str(count) + " records have been inserted into the parquet file '" + parquet_filename + "' in " + bucket_parquet
        
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

def nonesafe_dumps(obj):
    if obj != None or not np.isnan(obj):
        return json.dumps(obj)
    else:
        return np.nan

def query_uuid(uuid, popularity_table, region):
    
    dynamodb_client = boto3.client('dynamodb', region_name=region)
    
    try:
        response = dynamodb_client.query(
            TableName=popularity_table,
            KeyConditionExpression='#uuid = :uuid',
            ExpressionAttributeNames={
              '#uuid': 'uuid'
            },
            ExpressionAttributeValues={
              ':uuid': {'S':uuid}
            }
        )
    except ClientError as e:
        print(e)
    else:
        return response
        
def replace_decimals_dynamodb(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals_dynamodb(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj:
            obj[k] = replace_decimals_dynamodb(obj[k])
        return obj
    elif isinstance(obj, float):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


def dynamodb_table_to_df(table_name):
    """
    his function takes the name of a DynamoDB table as input, fetches all items from the table using the scan method, 
    and then creates a pandas DataFrame from the items. The scan operation in DynamoDB returns a maximum of 1 MB of data,
    which can not capture all the data. To retrieve all items, we use pagination. 
    
    """
    # Create a DynamoDB resource
    dynamodb = boto3.resource('dynamodb')

    # Get the table
    table = dynamodb.Table(table_name)

    # Initialize an empty list to hold all items
    items = []

    # Fetch the first page of items
    response = table.scan()

    # Add the items to our list
    items.extend(response['Items'])

    # While there are more items, fetch the next page and add its items to our list
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    # Create a pandas DataFrame from the items
    df = pd.DataFrame(items)
    print(f'The dynamoDB table {table_name} is load as a dataframe. The shape of the dataframe is {df.shape}')
    return df