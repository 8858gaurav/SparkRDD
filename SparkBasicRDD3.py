import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)


if __name__ == '__main__':
   print("creating spark session")


   spark = SparkSession \
           .builder \
           .appName("debu application") \
           .config("spark.sql.shuffle.partitions", 3) \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .config("spark.driver.bindAddress","localhost") \
           .config("spark.ui.port","4040") \
           .master("local[*]") \
           .getOrCreate()
   spark.sparkContext.setLogLevel('WARN')


   rdd1 = spark.sparkContext.textFile("/Users/gauravmishra/Desktop/Adding/SparkRDD/datasets/orders.csv")
   #['1,2013-07-25 00:00:00.0,11599,CLOSED', '2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT']  rdd1.take(1), rdd1.take(2)
   
   rdd2 = rdd1.map(lambda x: (int(x.split(",")[0]) *10, x.split(",")[1], x.split(",")[2], x.split(",")[3]))
   #rdd2.takeOrdered(2)): [(10, '2013-07-25 00:00:00.0', '11599', 'CLOSED'), (20, '2013-07-25 00:00:00.0', '256', 'PENDING_PAYMENT')]
   
   rdd3 = rdd2.filter(lambda x: x[3] == "CLOSED")
   #rdd3.takeOrdered(2)): [(10, '2013-07-25 00:00:00.0', '11599', 'CLOSED'), (40, '2013-07-25 00:00:00.0', '8827', 'CLOSED')]
  
   # countByValue() returns a dictionary with the count of each unique value in the RDD
   print(rdd3.map(lambda x: x[3]).countByValue())

   print("rdd2", rdd2.count(), "rdd3", rdd3.count())
   rdd4 = rdd3.map(lambda x: (x[2], 1))
   rdd5 = rdd4.reduceByKey(lambda x, y: x + y)

   rdd6 = spark.sparkContext.parallelize((1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
   print("rdd6", rdd6.reduce(lambda x, y: x + y))  # Output: 55

   rdd5.saveAsTextFile("/Users/gauravmishra/Desktop/Adding/SparkRDD/Output/orders_closed.txt")
