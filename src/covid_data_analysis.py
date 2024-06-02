import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
import matplotlib.pyplot as plot
import pandas as pd
import plotly.express as px

spark = SparkSession \
    .builder \
    .appName('PySpark Structure Streaming With Kafka And Message Format as CSV Data') \
    .master('local[*]') \
    .getOrCreate()

covid_schema = "state STRING, country STRING, lat DOUBLE, long DOUBLE, date STRING, " \
               + "confirmed INT, death INT, recovered INT, active INT, who_region STRING"

while True:
    covid_df = spark.read.csv('C:/Learning/Spark Streaming/output_path/', schema=covid_schema)

    month_year_extract_df = covid_df.withColumn('months',date_format('date','yyyy-MM'))
    grouped_months_df = month_year_extract_df.groupBy('months', 'country').sum('confirmed').orderBy('months')
    # pivot_df = grouped_months_df.groupBy('country').pivot('months').sum('sum(confirmed)')
    # pivot_df = pivot_df.na.fill(value=0)
    # pivot_df.show()

    # pandadf = pivot_df.toPandas()
    # pandadf.plot(x='country', kind='bar', stacked=False, title='Groupend bar chart')
    # plot.show()

    grouped_months_df.show()
    pandadf = grouped_months_df.toPandas()

    fig = px.scatter(pandadf, x='sum(confirmed)', y='months', color='country')
    fig.show()
    time.sleep(30)
