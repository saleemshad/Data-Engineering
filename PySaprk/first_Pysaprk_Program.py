###initializaed the Spark Session here
from pyspark.sql.import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. \
builder. \
config('spark.ui.port','0'). \
config("spark.sql.warehouse.dir",f"/user/itv000173/warehouse"). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate

# now read the file from data lake

rdd1 = spark.sparkContext.textFile("/usr/IT/data/inputfile.txt")

#perform the transformations

rdd2 = rdd1.flatMap( lamabda x : x.split(" "))

rdd3 = rdd2.map(lambda word : (word,1))

rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

# display the result back to the driver
rdd4.collect()
