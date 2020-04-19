#
#Lambda function used to write inbound IoT sensor data to RDS PostgeSQL database
#
import sys
import os
import json
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Lambda handler
def handler(event, context):
    processBatch(event)

#Connect to PostgreSQL and loop through batch submitted by Lambda poller
def processBatch(event):
    #No exception handling is used on the connection attempt because we want failed connection 
    #attempts to fail the function so that messages go back into the queue
    conn = psycopg2.connect(host=os.environ['rds_host'], port=os.environ['rds_port'], dbname=os.environ['rds_dbname'], 
                            user=os.environ['rds_username'], password=os.environ['rds_password'])
    conn.autocommit = True

    cur = conn.cursor()
    
    for record in event['Records']:
        try:
            payload_dict = json.loads(record['body'])
            cur.execute('insert into "SensorData" ("DeviceID", "DateTime", "Temperature", "Humidity", ' 
                        '"WindDirection", "WindIntensity", "RainHeight") values (%s, %s, %s, %s, %s, %s, %s)', 
                        (payload_dict['deviceid'], payload_dict['datetime'], payload_dict['temperature'], 
                         payload_dict['humidity'], payload_dict['windDirection'], payload_dict['windIntensity'], 
                         payload_dict['rainHeight']))
        except psycopg2.IntegrityError:
            #Duplicate primary key errors occur if Lambda calls this function more than once with the same message
            #Preventing duplicate inserts and ignoring failed attempts allows this function to be idempotent
            logger.info('PostgreSQL integrity error: DeviceID=' + payload_dict['deviceid'] + 
                        ', DateTime=' + payload_dict['datetime'])
        except:
            cur.close()
            conn.close()
            raise            
        
    cur.close()
    conn.close()

