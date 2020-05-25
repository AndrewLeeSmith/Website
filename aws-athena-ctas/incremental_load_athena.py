#
#Script for Lambda function that ETLs individual IoT message files into a
#Parquet, compressed, partitioned table defined in Amazon Athena.
#It's called hourly by EventBridge
#
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena_client = boto3.client('athena')
dynamodb_client = boto3.client('dynamodb')
s3_resource = boto3.resource('s3')

#SQL for ETL from staging into Parquet table
sql_insert_into = ('insert into iot_sensor_data.iot_data_processed ' 
    '(deviceid, datetime, temperature, humidity, winddirection, windintensity, rainheight, day) '
    'select deviceid, datetime, temperature, humidity, winddirection, '
    'windintensity, rainheight, date_format(datetime, \'%Y-%m-%d\') as day '
    'from iot_data_staging ')

messages_bucket = s3_resource.Bucket('iot-sensordata-messages')
staging_bucket = s3_resource.Bucket('iot-sensordata-staging')
glue_database = 'iot_sensor_data'
query_output_location = 's3://iot-sensordata-processed/partitioned/'
dynamodb_tablename = 'iot_sensordata_etl_queries'

#Lambda Handler that runs hourly via EventBridge scheduled rule
def lambda_handler(event, context):
    action = get_last_run_status()  
   
    if action == "DeleteStaging":
        empty_staging()
    
    if action != "DontContinue":
        files_in_staging = move_to_staging()

        if files_in_staging:
            load_from_staging()
 
#Retrieve the query execution details from the last hourly run of this script to see if it completed
def get_last_run_status():

    #Retrieve Athena query id of last incremental load
    response = dynamodb_client.get_item(
        TableName=dynamodb_tablename,
        Key={'query_execution_id': {
            'S': 'query_id'
            }
        }
    )
    
    #This will remain unassigned on the first run ever of the script
    job_state = ''
    
    #Check if last incremental load was successful and delete staging files if so
    #Else determine how to proceed with rest of ETL
    if 'Item' in response:
        query_id = response['Item']['value']['S']
        
        if query_id == 'ETLINPROGRESS':
            #This script failed last time after staging and before loading so don't delete staging 
            job_state = 'ETLINPROGRESS'
        else:            
            response = athena_client.get_query_execution(QueryExecutionId=query_id)
            job_state = response['QueryExecution']['Status']['State']

    if job_state != 'SUCCEEDED':
        logger.info('Last job state: ' + job_state)
        
    if job_state == 'SUCCEEDED':
        return "DeleteStaging"
    elif job_state in ['', 'FAILED', 'CANCELLED', 'ETLINPROGRESS']: 
        return 'DontDeleteStaging'
    elif job_state in ['QUEUED', 'RUNNING']:
        return 'DontContinue'
    else:
        return 'DontContinue'

#Delete all the files in staging since they must have been loaded
#An alternative to deleting is to archive the files to another bucket
def empty_staging():

    key_list = []
    for obj in staging_bucket.objects.all():
        key_list.append({'Key': obj.key})
        
    if key_list:
        staging_bucket.delete_objects(
            Delete={'Objects': key_list}
        )
            
#Move all the latest IoT message files that have arrived into staging
def move_to_staging():    

    for obj in messages_bucket.objects.all():
        copy_source = {
            'Bucket': messages_bucket.name,
            'Key': obj.key
        }
        staging_bucket.copy(copy_source, obj.key)
    
    #Use the list of keys copied to the staging bucket for the delete operation, to make sure
    #we don't delete any files that have arrived in the source since the copy above completed
    key_list = []
    for obj in staging_bucket.objects.all():
        key_list.append({'Key' : obj.key})

    if key_list:
        #Update DynamoDB item so that if a script failure occurs after this point,
        #any retry will not delete the staged files that have not yet been loaded
        update_dynamodb('ETLINPROGRESS')
 
        messages_bucket.delete_objects(
            Delete={'Objects': key_list}
        )
        return True
    else:
        logger.info('No files in staging')
        return False
        
#Load all files from staging into the Parquet table, and write the Athena query id
#into DynamoDB. This is checked for success at the beginning of the next hourly run
def load_from_staging():
    
    response = athena_client.start_query_execution(
        QueryString=sql_insert_into,
        QueryExecutionContext={
            'Database': glue_database
        },
        ResultConfiguration={
            'OutputLocation': query_output_location
        },
        WorkGroup='primary'
    )

    query_id = response['QueryExecutionId']
    
    #Write the query id into DynamoDB so we can check it for success next time
    update_dynamodb(query_id)

#Update the single item in the DynamoDb table
def update_dynamodb(query_id_or_status):
    
    dynamodb_client.put_item(
        TableName = dynamodb_tablename, 
        Item =  {'query_execution_id': {
                    'S': 'query_id',
                },
                 'value' : {
                    'S': query_id_or_status,
                }
        }
    )    
