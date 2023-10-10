import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1695929332312 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://gluetutorial/flights.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1695929332312",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1695929346331 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1695929332312,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-029426086766-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "flightdetails.flights",
        "connectionName": "rcon_flights",
        "preactions": "DROP TABLE IF EXISTS flightdetails.flights; CREATE TABLE IF NOT EXISTS flightdetails.flights (YEAR VARCHAR, MONTH VARCHAR, DAY VARCHAR, DAY_OF_WEEK VARCHAR, AIRLINE VARCHAR, FLIGHT_NUMBER VARCHAR, TAIL_NUMBER VARCHAR, ORIGIN_AIRPORT VARCHAR, DESTINATION_AIRPORT VARCHAR, SCHEDULED_DEPARTURE VARCHAR, DEPARTURE_TIME VARCHAR, DEPARTURE_DELAY VARCHAR, TAXI_OUT VARCHAR, WHEELS_OFF VARCHAR, SCHEDULED_TIME VARCHAR, ELAPSED_TIME VARCHAR, AIR_TIME VARCHAR, DISTANCE VARCHAR, WHEELS_ON VARCHAR, TAXI_IN VARCHAR, SCHEDULED_ARRIVAL VARCHAR, ARRIVAL_TIME VARCHAR, ARRIVAL_DELAY VARCHAR, DIVERTED VARCHAR, CANCELLED VARCHAR, CANCELLATION_REASON VARCHAR, AIR_SYSTEM_DELAY VARCHAR, SECURITY_DELAY VARCHAR, AIRLINE_DELAY VARCHAR, LATE_AIRCRAFT_DELAY VARCHAR, WEATHER_DELAY VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1695929346331",
)

job.commit()
