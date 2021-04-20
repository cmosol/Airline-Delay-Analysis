from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
#create a spark session
spark = SparkSession.builder.appName('PredictDelay').getOrCreate()
#array of column names used to drop unneeded columns
columns_to_drop = ["OP_CARRIER", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "Unnamed: 27"]
#upload 2017.csv data to train
train = spark.read.option("header", True).csv(r"2017.csv")
#drop unneeded columns
for col in columns_to_drop:
    train = train.drop(col)
#upload 2018.csv data to test
test = spark.read.option("header", True).csv(r"2018.csv")
#drop unneeded columns
for col in columns_to_drop:
    test = test.drop(col)
#cast columns to type Integer
train = train.withColumn("ARR_DELAY", train["ARR_DELAY"].cast(IntegerType()))
train = train.withColumn("DEP_DELAY", train["DEP_DELAY"].cast(IntegerType()))
train = train.withColumn("CANCELLED", train["CANCELLED"].cast(IntegerType()))
train = train.withColumn("CARRIER_DELAY", train["CARRIER_DELAY"].cast(IntegerType()))
train = train.withColumn("WEATHER_DELAY", train["WEATHER_DELAY"].cast(IntegerType()))
train = train.withColumn("NAS_DELAY", train["NAS_DELAY"].cast(IntegerType()))
train = train.withColumn("SECURITY_DELAY", train["SECURITY_DELAY"].cast(IntegerType()))
train = train.withColumn("LATE_AIRCRAFT_DELAY", train["LATE_AIRCRAFT_DELAY"].cast(IntegerType()))
test = test.withColumn("ARR_DELAY", test["ARR_DELAY"].cast(IntegerType()))
test = test.withColumn("DEP_DELAY", test["DEP_DELAY"].cast(IntegerType()))
test = test.withColumn("CANCELLED", test["CANCELLED"].cast(IntegerType()))
test = test.withColumn("CARRIER_DELAY", test["CARRIER_DELAY"].cast(IntegerType()))
test = test.withColumn("WEATHER_DELAY", test["WEATHER_DELAY"].cast(IntegerType()))
test = test.withColumn("NAS_DELAY", test["NAS_DELAY"].cast(IntegerType()))
test = test.withColumn("SECURITY_DELAY", test["SECURITY_DELAY"].cast(IntegerType()))
test = test.withColumn("LATE_AIRCRAFT_DELAY", test["LATE_AIRCRAFT_DELAY"].cast(IntegerType()))
#Show resulting dataframe
train.show(10)
test.show(10)
#assemble input columns in vector
assembler = VectorAssembler(inputCols = ["DEP_DELAY", "ARR_DELAY", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"], outputCol='features')
#transform data with vector
train = assembler.setHandleInvalid("skip").transform(train)
test = assembler.setHandleInvalid("skip").transform(test)
#build train and run linear regression
algo = LinearRegression(featuresCol="features", labelCol="CANCELLED", regParam=0.3, maxIter=10, elasticNetParam=0.8)
model = algo.fit(train)
predictions = model.transform(test).show(10)
#build train and run logistic regression
lr = LogisticRegression(featuresCol = 'features', labelCol = "CANCELLED", maxIter=10)
lrModel = lr.fit(train)
lrpredictions = lrModel.transform(test).show()
#build train and run Decision Tree regression
dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'CANCELLED')
dt_model = dt.fit(train)
dt_predictions = dt_model.transform(test)
dt_evaluator = RegressionEvaluator(labelCol="CANCELLED", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)