#
#Lambda function used to write inbound IoT sensor data to RDS PostgeSQL database.
#
import sys
import os
import json
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def make_connection():
    conn = psycopg2.connect(host=os.environ['rds_host'], port=os.environ['rds_port'], dbname=os.environ['rds_dbname'], 
                            user=os.environ['rds_username'], password=os.environ['rds_password'])
    conn.autocommit = True
    return conn

logger.info("Cold start complete.")

def handler(event, context):

    try:
        conn = make_connection()
        cur = conn.cursor()
        
        try:
            cur.execute('insert into "SensorData" ("DeviceID", "DateTime", "Temperature", "Humidity", ' 
                        '"WindDirection", "WindIntensity", "RainHeight") values (%s, %s, %s, %s, %s, %s, %s)', 
                        (event['deviceid'], event['datetime'], event['temperature'], event['humidity'], 
                         event['windDirection'], event['windIntensity'], event['rainHeight']))
            cur.close()
            
        except:
            logger.exception("ERROR: Cannot execute cursor.")
            return
            
        return "Successfully inserted 1 row."
                
    except:
        logger.exception("ERROR: Cannot connect to database.")
        return
            
    finally:
        try:
            conn.close()
        except:
            pass

#Used when testing from the Linux command line    
#if __name__== "__main__":
#    handler(None, None)
    