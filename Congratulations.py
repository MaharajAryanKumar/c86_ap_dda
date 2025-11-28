# 1. Setup Configuration
# Define the S3 endpoint (This is specific to your internal network)
endpoint = "https://s3-sg-p1-scc.fg.rbc.com/"

# Configure the Spark Context (sc) to use the correct endpoint and credentials
# Note: 'sc' is usually automatically defined in JupyterHub Spark notebooks. 
# If not, use: sc = spark.sparkContext
sc.jsc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint)

# Set the path to the JCEKS file which contains your actual secret keys/passwords
# IMPORTANT: Ensure this path is accessible from your user account
jceks_path = "jceks://hdfs/prod/05236/app/X610/DABI/CRMMR/keys/jcek_file_Spark.jceks"
sc.jsc.hadoopConfiguration().set("hadoop.security.credential.provider.path", jceks_path)

# ---------------------------------------------------------

# 2. Read from Parquet
# Reading a specific file from the S3 bucket into a DataFrame (df)
input_path = "s3a://s3-mr-sas-migration/SAS_Dataset_Conversions/cdm_sas_converter_20241122203732/sas/cdm/12_Channel_Reporting/MMC/mmc_save_queue.sas7bdat/mmc_save_queue_1.parquet"

df = spark.read.parquet(input_path)

# Optional: View the first few rows to verify read
df.show(5)

# ---------------------------------------------------------

# 3. Write to S3
# Writing a DataFrame to an S3 path. 
# Note: The slide uses 'df_teradata', assuming you have a dataframe with that name ready.
# If you just want to write back the data you read, change 'df_teradata' to 'df'.

output_s3_path = "s3a://s3-mr-sas-migration/output_folder_name/" # Added a subfolder for safety

# Write the data in parquet format, overwriting if it exists
df_teradata.write.mode("overwrite").parquet(output_s3_path)
