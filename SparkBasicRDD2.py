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

   
   rdd1 = spark.sparkContext.textFile("/Users/gauravmishra/Desktop/Adding/SparkRDD/datasets/inputfile.txt")
   print("rdd1", rdd1)
   print("rdd1 count", rdd1.count())
   # print each line in a single string inside a list
   print("rdd1 collect", rdd1.collect())
   # print the first line from a list
   print("rdd1 first", rdd1.first())
   # print the first 5 lines from a list, all the 5 lines are in a list, seperated by comma
   print("rdd1 take(5)", rdd1.take(5))
   print("rdd1 takeOrdered(5)", rdd1.takeOrdered(5))
   # take the 5 element as a line from a list, & all the 5 lines are in a list, seperated by comma
   print("rdd1 top(5)", rdd1.top(5))




   # flatMap: split all the lines into words by space, place all the words in a single list
   # e.g ['A', 'paragraph', 'is', 'a', 'distinct', 'unit', 'of', 'writing,', 'typically', ..... ]
   rdd2 = rdd1.flatMap(lambda x: x.split(" "))
   print("rdd2", rdd2.collect())


   #words count


   # e.g [('A', 1), ('paragraph', 1), ('is', 1), ('a', 1), ('distinct', 1), ('unit', 1), ('of', 1), ('writing,', 1), ...]
   rdd3 = rdd2.map(lambda x: (x.lower(), 1))
   print("rdd3", rdd3.collect())


   # reduceByKey: sum the count of each word
   # e.g [('distinct', 1), ('unit', 1), ('of', 4), ... ]
   rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
   print("rdd4", rdd4.collect())


   # saving the rdd
   rdd4.saveAsTextFile("/Users/gauravmishra/Desktop/Adding/SparkRDD/Output/file.txt")


   print("no of cpu cores available in this machine",spark.sparkContext.defaultParallelism)
   print(spark.sparkContext.defaultMinPartitions)
  
   print("rdd4 partitions", rdd4.getNumPartitions())
   print("rdd1 partitions", rdd1.getNumPartitions())


   print("spark conf", print(spark.sparkContext.getConf().getAll()))
   # output of spark conf will be a list of tuples, each tuple contains key and value
   # [('spark.rdd.compress', 'True'),
   # ('spark.hadoop.fs.s3a.vectored.read.min.seek.size', '128K'),
   # ('spark.app.startTime', '1751891771748'),
   # ('spark.executor.extraJavaOptions', '-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true'),
   # ('spark.sql.artifact.isolation.enabled', 'false'),
   # ('spark.ui.port', '4040'),
   # ('spark.app.id', 'local-1751891772199'),
   # ('spark.driver.bindAddress', 'localhost'),
   # ('spark.master', 'local[*]'),
   # ('spark.driver.host', '192.168.1.3'),
   # ('spark.sql.shuffle.partitions', '3'),
   # ('spark.sql.warehouse.dir', '/user/gauravmishra/warehouse'),
   # ('spark.driver.port', '57459'),
   # ('spark.executor.id', 'driver'),
   # ('spark.app.name', 'debu application'),
   # ('spark.submit.pyFiles', ''),
   # ('spark.driver.extraJavaOptions', '-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.incubator.vector --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true'),
   # ('spark.hadoop.fs.s3a.vectored.read.max.merged.size', '2M'),
   # ('spark.app.submitTime', '1751891771446'),
   # ('spark.submit.deployMode', 'client'),
   # ('spark.serializer.objectStreamReset', '100'),
   # ('spark.sql.catalogImplementation', 'hive'),
   # ('spark.ui.showConsoleProgress', 'true')]
