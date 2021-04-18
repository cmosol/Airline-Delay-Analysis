from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

#create a spark session
spark = SparkSession.builder.appName('AverageDelay').getOrCreate()

df1 = spark.read.option("header", True).csv('/media/christopher/14B5-FAA1/2018.csv')
df1 = df1.withColumn("CANCELLED", df1["CANCELLED"].cast(IntegerType()))

canc = df1.filter(df1["CANCELLED"] == 1)
notCanc= df1.filter(df1["CANCELLED"] == 0)

cancRDD = canc.rdd
notCancRDD = notCanc.rdd

alt = cancRDD.map(lambda row: matchAlternate(row, notCancRDD))

def matchAlternate(cRow,ncRDD):
    date=cRow.FL_DATE
    origin=cRow.ORIGIN
    dest=cRow.DEST
    return ncRDD.map(lambda ncRow: match(date, origin, dest, cRow, ncRow))

def match(date, origin, dest, cr, ncr):
    date2=ncr.FL_DATE
    origin2=ncr.ORIGIN
    dest2=ncr.DEST
    if (date == date2 and origin == origin2 and dest == dest2):
        c = cr.toDF()
        nc = ncr.toDF()
        c = c.withColumn("alt_date", nc["FL_DATE"]).withColumn("alt_carrier", nc["OP_CARRIER"]).withColumn("alt_flt_num", nc["OP_FL_NUM"]).withColumn("alt_deptime", nc["DEP_TIME"])
    else:
        c = cr.toDF()
        nc = ncr.toDF()
        c = c.withColumn("alt_date", "N/A").withColumn("alt_carrier", "N/A").withColumn("alt_flt_num", "N/A").withColumn("alt_deptime", "N/A")
    return c
