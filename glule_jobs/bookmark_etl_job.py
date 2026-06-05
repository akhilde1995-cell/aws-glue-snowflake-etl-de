from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, row_number, input_file_name, desc
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bookmark-etl-job", {})

# Iceberg configuration
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://tgt-bucket-bj/iceberg-warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Read CSV source files
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://raw-bucket-bj/employees/")

# Clean data
df_clean = df.select(
    col("id").cast("int").alias("id"),
    col("name").cast("string").alias("name"),
    col("salary").cast("long").alias("salary")
)

df_clean = df_clean.withColumn(
    "salary",
    when(col("salary") < 0, None).otherwise(col("salary"))
)

df_clean = df_clean.dropna(subset=["id", "name", "salary"])

# Deduplicate by latest source file
df_clean = df_clean.withColumn("source_file", input_file_name())

window_spec = Window.partitionBy("id").orderBy(desc("source_file"))

df_dedup = df_clean.withColumn(
    "rn",
    row_number().over(window_spec)
).filter(
    col("rn") == 1
).select(
    col("id"),
    col("name"),
    col("salary")
)

# IMPORTANT: materialize source to remove non-deterministic lineage
staging_path = "s3://tgt-bucket-bj/temp/source_employees_stage/"

df_dedup.write \
    .mode("overwrite") \
    .parquet(staging_path)

source_df = spark.read.parquet(staging_path)

source_df.createOrReplaceTempView("source_employees")

# Create Iceberg database
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.employee_iceberg_db
""")

# Create Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.employee_iceberg_db.employees (
    id INT,
    name STRING,
    salary BIGINT
)
USING iceberg
LOCATION 's3://tgt-bucket-bj/employees_iceberg/'
""")

# MERGE into Iceberg table
spark.sql("""
MERGE INTO glue_catalog.employee_iceberg_db.employees AS target
USING source_employees AS source
ON target.id = source.id

WHEN MATCHED THEN UPDATE SET
    name = source.name,
    salary = source.salary

WHEN NOT MATCHED THEN INSERT (
    id,
    name,
    salary
)
VALUES (
    source.id,
    source.name,
    source.salary
)
""")

print("Iceberg MERGE completed successfully")

job.commit()