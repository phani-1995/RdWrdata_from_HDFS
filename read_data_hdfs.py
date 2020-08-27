from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructType, StructField
spark=SparkSession \
    .builder \
    .getOrCreate()
sc=spark.sparkContext
schema = StructType([StructField("Flight Number", StringType(), True),
                      StructField("Launch Date", StringType(), True),
                      StructField("Launch Site", StringType(), True),
                      StructField("Vehicle Type", StringType(), True),
                      StructField("Payload Name", StringType(), True),
                      StructField("Payload Type", StringType(), True),
                      StructField("Customer Type", StringType(), True),
                      StructField("Customer Country", StringType(), True),
                      StructField("Mission Outcome", StringType(), True),
                      StructField("Failure Reason", StringType(), True)])
def read_data():
    input_path = "hdfs://localhost:54310/user/data/space_missions1/*.json"
    missions = spark\
        .read\
        .schema(schema)\
        .json(input_path)
    #Filling missing values
    clean_df = missions.na.fill('data missing')
    return clean_df.show(3)
#def filter_data():

if __name__ == '__main__':
    read_data()
