from pyspark.sql import SparkSession, DataFrame
from config.config import configuration
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import udf, regexp_replace, col, regexp_extract, expr
from udf_utils import *


def define_udfs():
    return {
        'extract_file_name_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
        'extract_salary_udf': udf(extract_salary, StructType([
            StructField('salary_start', DoubleType(), True),
            StructField('salary_end', DoubleType(), True)
        ])),
        'extract_start_date_udf': udf(extract_start_date, DateType()),
        'extract_end_date_udf': udf(extract_end_date, DateType()),
        'extract_class_code_udf': udf(extract_class_code, StringType()),
        'extract_requirements_udf': udf(extract_requirements, StringType()),
        'extract_notes_udf': udf(extract_notes, StringType()),
        'extract_duties_udf': udf(extract_duties, StringType()),
        'extract_selection_udf': udf(extract_selection, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),
        'extract_education_length_udf': udf(extract_education_length, StringType()),
        'extract_application_location_udf': udf(extract_application_location, StringType()),
    }
@udf(StringType())
def extract_file_name1(file_content):
    file_content = file_content.strip()
    lines = file_content.split('\n')
    title = lines[0]
    return title

def streamWriter(input: DataFrame, checkpointFolder, output):
    return (input.writeStream.
            format('parquet')
            .option('checkpointLocation', checkpointFolder)
            .option('path', output)
            .outputMode('append')
            .trigger(processingTime='5 seconds')
            .start()
            )

if __name__ == "__main__":
    spark = (SparkSession.builder.config("spark.driver.host", "localhost").appName('AWS_Spark_Unstructured')
             .config('spark.jars.packages',
                     'org.apache.hadoop:hadoop-aws:3.3.1,'
                     'com.amazonaws:aws-java-sdk:1.11.469')
             .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
             .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))
             .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))
             .config('spark.hadoop.fs.s3a.aws.credentials.provider',
             'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
             .getOrCreate()
             )

    text_input_dir = "file:///C:/Users/neera/pycharmprojects/AWS_Spark_Unstructured/input_text"
    json_input_dir = "file:///C:/Users/neera/pycharmprojects/AWS_Spark_Unstructured/input_json"
    pdf_input_dir = "file:///C:/Users/neera/pycharmprojects/AWS_Spark_Unstructured/input_pdf"
    video_input_dir = "file:///C:/Users/neera/pycharmprojects/AWS_Spark_Unstructured/input_video"
    image_input_dir = "file:///C:/Users/neera/pycharmprojects/AWS_Spark_Unstructured/input_image"
    csv_input_dir = "file:///C:/Users/neera/pycharmprojects/AWS_Spark_Unstructured/input_csv"

    data_schema = StructType([
        StructField('file_name', StringType(), True),
        StructField('position', StringType(), True),
        StructField('class_code', StringType(), True),
        StructField('salary_start', DoubleType(), True),
        StructField('salary_end', DoubleType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('req', StringType(), True),
        StructField('notes', StringType(), True),
        StructField('duties', StringType(), True),
        StructField('selection', StringType(), True),
        StructField('experience_length', StringType(), True),
        StructField('job_type', StringType(), True),
        StructField('education_length', StringType(), True),
        StructField('application_location', StringType(), True)
    ])

    udfs = define_udfs()
    test= extract_file_name1()

    job_bulletins_df = (spark.readStream
                        .format('text')
                        .option('wholetext', 'true')
                        .load(text_input_dir)
                        )

    #job_bulletins_df = job_bulletins_df.withColumn("file_name", regexp_replace(extract_file_name1("value"), r'\r', ' '))

    job_bulletins_df = job_bulletins_df.withColumn("file_name", expr("split(value, '\n')[0]"))
    job_bulletins_df = job_bulletins_df.withColumn("file_name", regexp_replace("file_name", r'\r', ' '))
    job_bulletins_df = job_bulletins_df.select("file_name")
    json_df = spark.readStream.json(json_input_dir, schema=data_schema, multiLine=True)
    json_df = json_df.select("file_name")
    union_dataframe = job_bulletins_df.union(json_df)

    # query = (union_dataframe
    #          .writeStream
    #          .outputMode('append')
    #          .format('console')
    #          .option('truncate', 'false')
    #          .start()
    #          )

    query = streamWriter(union_dataframe, 's3a://spark-unstructured-streaming-1891/checkpoints/',
                         's3a://spark-unstructured-streaming-1891/data/spark_unstructured')

    query.awaitTermination()
    spark.stop()
