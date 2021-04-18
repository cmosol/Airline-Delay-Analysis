from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
from pyspark.sql.types import IntegerType

#create a spark session
spark = SparkSession.builder.appName('CancelledFlights').getOrCreate()

#create data frame from flight data
df = spark.read.option("header", True).csv('file input path')
#cast ARR_DELAY and DEP_DELAY to integer
df = df.withColumn("ARR_DELAY", df["ARR_DELAY"].cast(IntegerType()))
df = df.withColumn("DEP_DELAY", df["DEP_DELAY"].cast(IntegerType()))
#group by airline carrier, avg all ARR_DELAY per carrier, order cancellation total in asc order
df.groupBy("OP_CARRIER").avg("ARR_DELAY").orderBy(asc("avg(ARR_DELAY)")).show()
#group by airline carrier, avg all DEP_DELAY per carrier, order cancellation total in asc order
df.groupBy("OP_CARRIER").avg("DEP_DELAY").orderBy(asc("avg(DEP_DELAY)")).show()