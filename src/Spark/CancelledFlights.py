from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum

#create a spark session
spark = SparkSession.builder.appName('CancelledFlights').getOrCreate()
#create data frame from flight data
df = spark.read.option("header", True).csv(r"2018.csv")
#cast cancelled column to integer
df = df.withColumn("CANCELLED", df["CANCELLED"].cast(IntegerType()))
#group by airline carrier, sum all cancellations per carrier, order cancellation total in asc order
df.groupBy("OP_CARRIER").sum("CANCELLED").orderBy(asc("sum(CANCELLED)")).show()