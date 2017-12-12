from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType

# Build SparkSession
spark = SparkSession.builder.master("local[*]").appName("ETL Pipeline").getOrCreate()

# Set Logging Level to WARN
spark.sparkContext.setLogLevel("WARN")

###############################################################
#######################   EXTRACT  ############################
###############################################################

# Clean data stored in /vagrant/data
source_data_file = "/vagrant/data/*"

# Define csv input schema
schema = StructType([
    StructField("symbol", StringType()),
    StructField("date", DateType()),
    StructField("open", DecimalType(precision=38, scale=2)),
    StructField("high", DecimalType(precision=38, scale=2)),
    StructField("low", DecimalType(precision=38, scale=2)),
    StructField("close", DecimalType(precision=38, scale=2)),
    StructField("volume", IntegerType()),
    StructField("adj_close", DecimalType(precision=38, scale=2))
])

data = spark.read.csv(source_data_file, schema=schema).cache()

count = data.count()

data.show()

print("Data points from files count: {}".format(count))

###############################################################
#######################   TRANSFORM  ##########################
###############################################################

### Transform date field into separate year, month, and day fields

from pyspark.sql.functions import udf


def extract_month(date):
    if date is not None:
        return int(date.month)


def extract_year(date):
    if date is not None:
        return int(date.year)


def extract_day(date):
    if date is not None:
        return int(date.day)


udf_month = udf(extract_month, IntegerType())
udf_year = udf(extract_year, IntegerType())
udf_day = udf(extract_day, IntegerType())

# Call .show() if you want to see sample data
tup = ("close", "adj_close")
df_with_month_year = data \
    .withColumn("month", udf_month("date")) \
    .withColumn("year", udf_year("date")) \
    .withColumn("day", udf_day("date"))

df_with_month_year.show()

df_with_month_year_count = df_with_month_year.count()
print("Data points after adding month, year, day: {}".format(df_with_month_year_count))

### Filter any NULL symbols
df2 = df_with_month_year.filter("symbol is not NULL")

df2.show()

df2_count = df2.count()
print("Data points remaining after removing nulls: {}".format(df2_count))

print("Removed {} nulls".format(df_with_month_year_count - df2_count))

###############################################################
##################   SOME DATA ANALYTICS  #####################
###############################################################

from pyspark.sql.functions import avg

monthly_avg_close_df = df2.groupBy(df2.month).agg(avg("close").alias("average_month_close")).orderBy(df2.month)

monthly_avg_close_df.show()

adj_close_diff_than_close = df2.filter("close != adj_close").groupBy(df2.month).count().orderBy(df2.month)

adj_close_diff_than_close.show()

###############################################################
#########################   LOAD  #############################
###############################################################

print("Starting DB write")

from pyspark.sql import DataFrameWriter


def write_df_to_table(df_writer, table):
    jdbc_url = "jdbc:postgresql://0.0.0.0:5432/postgres"
    mode = "overwrite"
    properties = {"user": "postgres",
                  "password": "mysecretpassword",
                  "driver": "org.postgresql.Driver"}

    print("Writing to {}".format(table))

    df_writer.jdbc(jdbc_url, table, mode, properties)


# Write df2
write_df_to_table(DataFrameWriter(df2), "stock_data")

# Write monthly average close
write_df_to_table(DataFrameWriter(monthly_avg_close_df), "avg_month_close")

# Write adj close diff than close count
write_df_to_table(DataFrameWriter(adj_close_diff_than_close), "adjusted_close_count")

print("DB Write complete")

print("Complete")
