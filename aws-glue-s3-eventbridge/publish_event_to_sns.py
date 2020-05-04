#
#Receive EventBridge and S3 events and format their contents into a message to publish to an SNS topic
#
import boto3

sns = boto3.client('sns')

# Determine AWS event type for relevant events
def get_event_source(event):
    if 'Records' in event and 'eventSource' in event['Records'][0] and event['Records'][0]['eventSource'] == 'aws:s3': 
        return 's3'
    elif 'source' in event and event['source'] == 'aws.glue':
        return 'glue'
    else:
        return 'unknown'

# Compose and publish SNS from AWS event info
def lambda_handler(event, context):
    sns_message = ''
    source = get_event_source(event)
    
    if source == 's3':
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_name = event['Records'][0]['s3']['object']['key']
        sns_message = 'The error file "' + file_name + '" has landed in S3 bucket "' + bucket_name + '".'
    elif source == 'glue':
        job_name = event['detail']['jobName']
        state = event['detail']['state']
        time = event['time']
        sns_message = 'Glue job "' + job_name + '" has ' + state + ' at ' + time + '.'
    
    if sns_message != '':
        sns.publish(
            Message = sns_message,
            TargetArn = 'arn:aws:sns:eu-west-2:560620020482:ETLJobErrors'
        )
    
    