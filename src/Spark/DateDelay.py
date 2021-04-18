from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, desc
from pyspark.sql.types import IntegerType

#create a spark session
spark = SparkSession.builder.appName('DateDelay').getOrCreate()

#create data frame from flight data
df = spark.read.option("header", True).csv('file input path')
#cast ARR_DELAY and DEP_DELAY to integer
df = df.withColumn("ARR_DELAY", df["ARR_DELAY"].cast(IntegerType()))
df = df.withColumn("DEP_DELAY", df["DEP_DELAY"].cast(IntegerType()))
#create new avg_delay column calculated between ARR_DELAY and DEP_DELAY
df=df.withColumn("avg_delay", (df["ARR_DELAY"]+df["DEP_DELAY"])/2)
#group by FL_DATE, avg avg_delay column, order avg_delay in asc order
df.groupBy("FL_DATE").avg("avg_delay").orderBy(asc("avg(avg_delay)")).show(10)
#group by FL_DATE, avg avg_delay column, order avg_delay in desc order
df.groupBy("FL_DATE").avg("avg_delay").orderBy(desc("avg(avg_delay)")).show(10)