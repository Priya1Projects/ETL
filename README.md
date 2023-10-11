# ETL
Data engineering 
# How ETL from sql db to Redshift
To start, we need to extract data from S3. After that, we'll identify the schema and write a Pyspark data enrichment process and data transformations. 
Next, we'll create a Redshift cluster with the right IAM role to be accessed from AWS Glue for loading the data. 
Once the cluster is set up, we'll load the data into Redshift. Finally, we can use Redshift credentials to connect and query through the editor in AWS console.


