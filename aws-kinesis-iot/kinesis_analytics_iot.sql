--Join the raw messages stream with reference data read from S3
CREATE OR REPLACE STREAM "MSGS_WITH_NAME_STREAM" ("deviceid" INT, "weatherstationname" VARCHAR(64), "datetime" TIMESTAMP, 
            "temperature" DECIMAL(4,1), "humidity" DECIMAL(4,1), "winddirection" DECIMAL(4,1), "windintensity" DECIMAL(4,1), 
            "rainheight" DECIMAL(4,1));

CREATE OR REPLACE PUMP "MSGS_STREAM_PUMP" AS INSERT INTO "MSGS_WITH_NAME_STREAM"
SELECT STREAM a."deviceid",
            w."weatherstationname",
            a."datetime",
            a."temperature",
            a."humidity",
            a."winddirection",
            a."windintensity",
            a."rainheight"
FROM "SOURCE_SQL_STREAM_001" AS a
INNER JOIN "WEATHER_STATION_NAMES" AS w
ON a."deviceid" = w."deviceid";

--Create rolling averages stream to send to Redshift table sensordata_avgs
CREATE OR REPLACE STREAM "KINESIS_FH_AVGS_STREAM" ("deviceid" INT, "weatherstationname" VARCHAR(64),  
            "min_datetime" TIMESTAMP, "avg_temperature" DECIMAL(4,1), "avg_humidity" DECIMAL(4,1), "msg_count" INT);

CREATE OR REPLACE PUMP "AVGS_STREAM_PUMP" AS INSERT INTO "KINESIS_FH_AVGS_STREAM"
SELECT STREAM "deviceid", 
            "weatherstationname",
            MIN("datetime") OVER W_SLIDING AS "min_datetime",
            AVG("temperature") OVER W_SLIDING AS "avg_temperature",
            AVG("humidity") OVER W_SLIDING AS "avg_humidity",
            COUNT(*) OVER W_SLIDING AS "msg_count"
FROM "MSGS_WITH_NAME_STREAM"
WINDOW W_SLIDING AS (
            PARTITION BY "deviceid", "weatherstationname"
            RANGE INTERVAL '1' MINUTE PRECEDING);

--Select rows for temperature alerts
CREATE OR REPLACE STREAM "TEMPERATURE_ALERTS_STREAM" ("deviceid" INT, "weatherstationname" VARCHAR(64),  
            "datetime" TIMESTAMP, "temperature" DECIMAL(4,1));

CREATE OR REPLACE PUMP "ALERTS_STREAM_PUMP" AS INSERT INTO "TEMPERATURE_ALERTS_STREAM"
SELECT STREAM "deviceid",
            "weatherstationname",
            "datetime",
            "temperature"
FROM "MSGS_WITH_NAME_STREAM"
WHERE "temperature" > 40;

--Create the temperature alerts stream to Kinsesis, to be read by a lambda function that publishes to SNS
CREATE OR REPLACE STREAM "KINESIS_ALERTS_STREAM" ("deviceid" INT, "weatherstationname" VARCHAR(64), 
            "min_datetime" TIMESTAMP, "max_datetime" TIMESTAMP, "min_temp" DECIMAL(4,1), "max_temp" DECIMAL(4,1), 
            "msg_count" INT);

--Filter the alerts so that there is at most 1 every 2 minutes
CREATE OR REPLACE PUMP "KINESIS_ALERTS_PUMP" AS INSERT INTO "KINESIS_ALERTS_STREAM"
SELECT STREAM "deviceid",
            "weatherstationname",
            MIN("datetime") AS "min_datetime",
            MAX("datetime") AS "max_datetime",
            MIN("temperature") AS "min_temp",
            MAX("temperature") AS "max_temp",
            COUNT(*) AS "msg_count"
FROM "TEMPERATURE_ALERTS_STREAM"
WINDOWED BY STAGGER (
            PARTITION BY "deviceid", "weatherstationname", STEP("datetime" BY INTERVAL '2' MINUTE)
            RANGE INTERVAL '2' MINUTE);
