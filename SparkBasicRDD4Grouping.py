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


   rdd1 = spark.sparkContext.textFile("/Users/gauravmishra/Desktop/Adding/SparkRDD-1/datasets/orders.csv")
   #['1,2013-07-25 00:00:00.0,11599,CLOSED', '2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT']  rdd1.take(1), rdd1.take(2)
   
   rdd2 = rdd1.map(lambda x: (int(x.split(",")[0]) *10, x.split(",")[1], x.split(",")[2], x.split(",")[3]))
   #rdd2.takeOrdered(2)): [(10, '2013-07-25 00:00:00.0', '11599', 'CLOSED'), (20, '2013-07-25 00:00:00.0', '256', 'PENDING_PAYMENT')]
   
   rdd3 = rdd2.filter(lambda x: x[3] != 'CLOSED')     
   #rdd3.takeOrdered(2)): [(10, '2013-07-25 00:00:00.0', '11599', 'CLOSED'), (40, '2013-07-25 00:00:00.0', '8827', 'CLOSED')]
  
   # countByValue() returns a dictionary with the count of each unique value in the RDD
   rdd7 = rdd3.map(lambda x: (x[2], x[3])).countByValue()
   print("rdd7", rdd3.map(lambda x: (x[2], x[3])).countByValue())
   # rdd7 defaultdict(<class 'int'>, {('11599', 'PENDING_PAYMENT'): 2, ('11599', 'COMPLETE'): 4, ('11599', 'PROCESSING'): 1, ('5648', 'PENDING_PAYMENT'): 1}

   print("rdd2", rdd2.count(), "rdd3", rdd3.count())
   rdd4 = rdd3.map(lambda x: (x[2], x[3]))
   rdd5 = rdd4.reduceByKey(lambda x, y: x + y)
   print("rdd5", rdd5.collect()) 
   # rdd5 [('11599', 'PENDING_PAYMENTCOMPLETECOMPLETECOMPLETECOMPLETEPROCESSINGPENDING_PAYMENT'), ('918', 'PAYMENT_REVIEW')]

   rdd6 = rdd3.map(lambda x: (x[2], x[3]))
   #[('11599', 'CLOSED'), ('8827', 'CLOSED')]
   print("rdd6", rdd6.groupByKey().mapValues(list).sortByKey().collect())
   # [('11599', ['PENDING_PAYMENT', 'COMPLETE', 'COMPLETE', 'COMPLETE', 'COMPLETE', 'PROCESSING', 'PENDING_PAYMENT']), ('11639', ['PENDING_PAYMENT'])]

   rdd8 = rdd6.groupByKey()
   # rdd8 ouput:[('11599', <pyspark.resultiterable.ResultIterable object at 0x102038830>), ('918', <pyspark.resultiterable.ResultIterable object at 0x10496a850>)]
   print("rdd8", rdd8.collect())
   print("rdd8", rdd8.map(lambda x: (x[0], len(x[1]))).sortByKey().collect())
   # [('11589', 1), ('11599', 7)]


   RDD4 = spark.sparkContext.parallelize([('a', 1), ('b', 2), ('a', 3), ('b', 4), ('a', 5)])
   for k, v in  RDD4.groupBy(lambda x: x[0]).collect():
        print(k, len([i[1] for i in list(v)])) 
        # a 3
        # b 2