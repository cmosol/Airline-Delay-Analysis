from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import expr, length
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import substring, concat

spark = SparkSession.builder.appName('PredictDelay').getOrCreate()

columns_to_drop = ["OP_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "Unnamed: 27"]
train = spark.read.option("header", True).csv(r"C:\Users\Christopher\Desktop\2017.csv")
for col in columns_to_drop:
    train = train.drop(col)
test = spark.read.option("header", True).csv(r"C:\Users\Christopher\Desktop\2018.csv")
for col in columns_to_drop:
    test = test.drop(col)

train = train.withColumn("ARR_DELAY", train["ARR_DELAY"].cast(IntegerType()))
train = train.withColumn("DEP_DELAY", train["DEP_DELAY"].cast(IntegerType()))
test = test.withColumn("ARR_DELAY", test["ARR_DELAY"].cast(IntegerType()))
test = test.withColumn("DEP_DELAY", test["DEP_DELAY"].cast(IntegerType()))

train["INDEX"] = train["FL_DATE"].str[5:7]+train["FL_DATE"].str[8:10]
test["INDEX"] = test["FL_DATE"].str[5:7]+test["FL_DATE"].str[8:10]

train = train.withColumn("INDEX", concat(substring("FL_DATE", 9,8),substring("FL_DATE", 9,9)))
test = test.withColumn("INDEX", concat(substring("FL_DATE", 9,8),substring("FL_DATE", 9,9)))

train = train.withColumn("INDEX", train["INDEX"].cast(IntegerType()))
test = test.withColumn("INDEX", test["INDEX"].cast(IntegerType()))

train.show(10)
test.show(10)

train = train.drop("FL_DATE")
test = test.drop("FL_DATE")
#train = spark.createDataFrame(train)
#test = spark.createDataFrame(test)


assembler1 = VectorAssembler(inputCols = ["DEP_DELAY", "ARR_DELAY", "INDEX"], outputCol='features')


train = assembler1.transform(train)
test = assembler1.transform(test)