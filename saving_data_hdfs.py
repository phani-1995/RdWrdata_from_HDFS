from pyspark.sql import SparkSession
import re
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()
sc = spark.sparkContext

df = spark.read.option('header', True).csv('/home/phani/datasets/spacemissions.csv')
# print(df.show(3))

df1 = df.select('Flight Number', 'Launch Date', 'Launch Site', 'Vehicle Type', 'Payload Name', 'Payload Type',
                'Customer Type', 'Customer Country', 'Mission Outcome', 'Failure Reason')

#df1.withColumn("Flight Number", F.regexp_replace(F.col("Flight Number"), "-", ""))

df1.fillna('data not found')
print(df1.show())


#saving the data in hdfs

df1.write.json("hdfs://localhost:54310/user/data/space_missions1", mode='append')
