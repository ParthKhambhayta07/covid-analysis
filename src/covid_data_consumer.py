from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,DateType,DoubleType,IntegerType
from pyspark.sql.functions import col, split, regexp_replace, from_csv, to_date, when, floor, date_format
import time

scala_version = '2.12'
spark_version = '3.0.3'
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.0'
]

KAFKA_TOPIC_NAME_CONS = 'covid_data'
KAFKA_BOOTSTRAP_SERVER_CONST = 'localhost:9092'

if __name__ == '__main__':
    print('Stream Data Processing Application Started...')
    print(time.strftime('%y-%m-%d %H:%M:%S'))

    spark = SparkSession \
            .builder \
            .appName('PySpark Structure Streaming With Kafka And Message Format as CSV Data') \
            .config("spark.jars.packages", ",".join(packages)) \
            .master('local[*]') \
            .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    covid_row_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER_CONST) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    print('Printing Schema of covid_df...')
    covid_row_df.printSchema()

    covid_row_value_df = covid_row_df.selectExpr("CAST(value AS STRING)")

    covid_schema = "state STRING, country STRING, lat DOUBLE, long DOUBLE, date STRING, " \
                   + "confirmed INT, death INT, recovered INT, active INT, who_region STRING"

    covid_row_value_df = covid_row_value_df.withColumn('value', regexp_replace(col('value'),'"',''))

    covid_schema_df = covid_row_value_df.select(from_csv(col("value"), covid_schema).alias("covid_data"))

    covid_schema_df = covid_schema_df.selectExpr("covid_data.*")

    covid_df = covid_schema_df.withColumn('date', to_date(col('date'), "yyyy-MM-dd").cast(DateType()))\
        .withColumn('confirmed', col('confirmed').cast(IntegerType()))

    covid_df.printSchema()

    # covid_max_case_df = covid_df.groupBy('country').sum('confirmed')\
    #     .select('country',col('sum(confirmed)').alias('confirmed_cases')).orderBy(col('confirmed_cases').desc())

    covid_conf_recv_case_df = covid_df.groupBy('country').\
        agg({'recovered': 'sum', 'confirmed': 'sum'}).\
        select('country',col('sum(confirmed)').alias('confirmed_cases'), col('sum(recovered)').alias('recovered_cases'))

    world_population_df = spark.read.csv('data/worldometer_data.csv', inferSchema=True, header=True)

    joined_df = covid_conf_recv_case_df.join(world_population_df, col('country') == col('Country/Region'))

    agg_covid_df = joined_df.withColumn('infectected_people / 1M', when(col('Population') > 0,
                                                               (col('confirmed_cases') / col('Population')) * 1000000).otherwise(0)) \
        .withColumn('infectection_rate', when(col('Population') > 0,(col('confirmed_cases') / col('Population')) * 100)
                    .otherwise(0)) \
        .withColumn('recovery_people / 1M', when(col('confirmed_cases') > 0,(col('recovered_cases') / col('confirmed_cases')) * 1000000)
                    .otherwise(0)) \
        .withColumn('recovery_rate', when(col('confirmed_cases') > 0,
                                          (col('recovered_cases') / col('confirmed_cases')) * 100).otherwise(0)) \
        .select('country', 'confirmed_cases', 'recovered_cases', 'Population', floor(col('infectected_people / 1M')),
                'infectection_rate',floor(col('recovery_people / 1M')), 'recovery_rate').orderBy(col('confirmed_cases').desc())

    # month_year_extract_df = covid_df.withColumn('months',date_format('date','yyyy-MM'))
    # grouped_months_df = month_year_extract_df.groupBy('months', 'country').sum('confirmed')
    # pivot_df = grouped_months_df.groupBy('country').pivot('months').sum('sum(confirmed)')
    # pivot_df = pivot_df.na.fill(value=0)

    covid_stream = agg_covid_df \
        .writeStream \
        .outputMode('complete') \
        .option('truncate', 'false') \
        .option('numRows', 5) \
        .format('console') \
        .trigger(processingTime='5 seconds') \
        .start()

    covid_chart_stream = covid_df.writeStream \
        .format("parquet") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "C:/Learning/Spark Streaming/checkpoint") \
        .option("path", "C:/Learning/Spark Streaming/output_path/") \
        .outputMode("append") \
        .start()

    covid_chart_stream.awaitTermination()

    covid_stream.awaitTermination()

    print('Stream Data Processing Application Completed...')

