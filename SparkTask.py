import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

def transform_data(data_source: str, output_uri: str) -> None:
    # Initialize SparkSession
    with SparkSession.builder.appName("Business Rides Data Transformation").getOrCreate() as spark:
        # Step 1: Read CSV file from S3
        df = spark.read.option("header", "true").csv(data_source)

        # Step 2: Perform data transformations
        # Convert columns to proper data types
        df = df.withColumn("MILES", col("MILES").cast("double"))

        # Rename columns for clarity
        df = df.withColumnRenamed("START", "start_location") \
               .withColumnRenamed("CATEGORY", "ride_category") \
               .withColumnRenamed("PURPOSE", "ride_purpose") \
               .withColumnRenamed("START_DATE", "start_date") \
               .withColumnRenamed("END_DATE", "end_date") \
               .withColumnRenamed("STOP", "end_location")

        # Create a new column based on conditions, if needed (e.g., ride length category)
        df = df.withColumn("ride_length_category", 
                           when(col("MILES") > 10, lit("long"))
                           .otherwise(lit("short")))

        # Step 3: Write intermediate results to HDFS
        hdfs_intermediate_path = "hdfs:///intermediate_data"
        df.write.mode("overwrite").parquet(hdfs_intermediate_path)  # Save in Parquet format for efficient storage

        print(f"Intermediate data written to HDFS at {hdfs_intermediate_path}")

        # Step 4: Read intermediate data from HDFS
        df_intermediate = spark.read.parquet(hdfs_intermediate_path)

        # Step 5: Write the final transformed data to S3 as a JSON file
        df_intermediate.write.mode("overwrite").json(f"{output_uri}/transformed_data.json")

        print(f"Transformed data written to {output_uri}/transformed_data.json")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_source", help="Path to the input CSV file")
    parser.add_argument("--output_uri", help="Path to save the transformed JSON file")
    args = parser.parse_args()

    transform_data(args.data_source, args.output_uri)
