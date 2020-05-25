#
#This script is used by a Lambda function to process alert events received from a Kinesis stream 
#It formats them into human readable messages and publishes them to an SNS topic
#
import json
import base64
import logging
import boto3

sns = boto3.client('sns')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    for record in event['Records']:
        byte_payload = base64.b64decode(record["kinesis"]["data"])
        logger.info(byte_payload)
        payload = json.loads(byte_payload)
        
        sns_message = ('Weather station "' + payload['weatherstationname'] + '" reported ' + str(payload['msg_count']) + 
                       ' alert(s) between ' + str(payload['min_temp']) + '°C and ' + str(payload['max_temp'])  +  
                       '°C from ' + payload['min_datetime'][0:19] + ' to ' + payload['max_datetime'][0:19] + '.'
        )
        
        sns.publish(
            Message = sns_message,
            TargetArn = 'arn:aws:sns:eu-west-2:560620020482:IoT_Temperature_Alerts'
        )

