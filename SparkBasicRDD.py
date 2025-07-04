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

    rdd1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    rdd2 = rdd1.map(lambda x: x + 2)
    print("RDD1: ", rdd1.collect())
    print("RDD2: ", rdd2.collect())

    rdd3 = rdd1.filter(lambda x: x > 2)
    print("RDD3: ", rdd3.collect())

    rdd4 = spark.sparkContext.parallelize(list(range(1, 3)))
    rdd5 = rdd4.map(lambda x: x >1)
    print("RDD4: ", rdd4.collect())
    print("RDD5: ", rdd5.collect())
    print("RDD6:", rdd4.flatMap(lambda x: range(0, x)).collect())
    print("RDD7:", rdd4.flatMap(lambda x: range(1, x * 3).collect()))


    RDD1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    RDD2 = spark.sparkContext.parallelize([6, 7, 8, 9, 10, 6, 7, 8, 9, 10])
    # don't use this in pyspark: spark.sparkContext.parallelize(list(1, 2)).distinct()
    
    print("DistinctRDD1",RDD1.distinct().collect(), type(RDD1), type(RDD1.distinct()), type(RDD1.distinct().collect()))
    print("DistinctRDD2",  RDD2.distinct().collect())

    print("UnionRDD", RDD1.distinct().union(RDD2.distinct()).collect())

    print("IntersectionRDD", RDD1.intersection(RDD2).collect())
    print("IntersectionDistinctRDD", RDD1.intersection(RDD2).distinct().collect())

    print("SubstractRDD", RDD1.subtract(RDD2).collect())
    print("SubstractDistinctRDD", RDD1.subtract(RDD2).distinct().collect())

    print("ReduceRDD", RDD1.reduce(lambda x, y: x + y))
    print("ReduceDistinctRDD", RDD1.distinct().reduce(lambda x, y: x + y))

    RDD3 = spark.sparkContext.parallelize(['gaurav', 'saurabh', 'ashish', 'ricky'])
    print(RDD3.filter(lambda x: 'a' in x).collect())
    print("sorting RDD3", RDD3.sortBy(lambda x: x, ascending= True).collect())
    print("RDD3 No Of Partitions:", RDD3.getNumPartitions())

    # Grouping and aggregating on RDD
    RDD4 = spark.sparkContext.parallelize([('a', 1), ('b', 2), ('a', 3), ('b', 4)])
    for k, v in  RDD4.groupBy(lambda x: x[0]).collect():
        print(k, ([i[1] for i in list(v)]))

    # Zip operation on RDD
    RDD5 = spark.sparkContext.parallelize([1, 2, 3])
    RDD6 = spark.sparkContext.parallelize(['a', 'b', 'c'])
    print("Zipped RDD:", RDD5.zip(RDD6).collect())

    # repartitioning RDD
    RDD7 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    RDD8 = RDD7.repartition(10)
    print("RDD7 Partitions:", RDD7.getNumPartitions())
    print("RDD8 Partitions:", RDD8.getNumPartitions())
    print("RDD7 Data:", RDD7.foreachPartition(lambda x: print(list(x))))
    print("RDD8 Data:", RDD8.foreachPartition(lambda x: print(list(x))))

    # Coalesce operation on RDD
    RDD9 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    RDD10 = RDD9.coalesce(1)
    print("RDD9 Partitions:", RDD9.getNumPartitions())
    print("RDD10 Partitions:", RDD10.getNumPartitions())
    print("RDD9 Data:", RDD9.foreachPartition(lambda x: print(list(x))))
    print("RDD10 Data:", RDD10.foreachPartition(lambda x: print(list(x))))


    # Mathematical operations on RDD
    RDD11 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    print("Sum of RDD11:", RDD11.sum())
    print("Mean of RDD11:", RDD11.mean())
    print("Max of RDD11:", RDD11.max()) 
    print("Min of RDD11:", RDD11.min())
    print("Count of RDD11:", RDD11.count())
    print("Variance of RDD11:", RDD11.variance())
    print("Standard Deviation of RDD11:", RDD11.stdev())


    # will create a new table in the warehouse directory, will create the same no of files as the no of partitions
    # saveAsTextFile will work only with RDDs, not with DataFrames
    RDD8.saveAsTextFile("/Users/gauravmishra/Desktop/adding/SparkRDD-2/output/output_rdd8")

    time.sleep(3600)  # Keep the Spark session alive for an hour
    spark.stop()
    print("Spark session stopped")