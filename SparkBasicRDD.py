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
    ## RDD1:  [1, 2, 3, 4, 5]
    ## RDD2:  [3, 4, 5, 6, 7]

    rdd3 = rdd1.filter(lambda x: x > 2)
    print("RDD3: ", rdd3.collect())
    ## RDD3:  [3, 4, 5]

    rdd4 = spark.sparkContext.parallelize(list(range(1, 3)))
    rdd5 = rdd4.map(lambda x: x >1)
    print("RDD4: ", rdd4.collect())
    print("RDD5: ", rdd5.collect())
    print("RDD6:", rdd4.flatMap(lambda x: range(0, x)).collect())
    print("RDD7:", rdd4.flatMap(lambda x: range(1, x * 3).collect()))
    # RDD4:  [1, 2]
    # RDD5:  [False, True]
    # RDD6: [0, 0, 1]
    # RDD7: PythonRDD[6] at RDD at PythonRDD.scala:53


    RDD1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    RDD2 = spark.sparkContext.parallelize([6, 7, 8, 9, 10, 6, 7, 8, 9, 10])
    # don't use this in pyspark: spark.sparkContext.parallelize(list(1, 2)).distinct()
    
    print("DistinctRDD1",RDD1.distinct().collect(), type(RDD1), type(RDD1.distinct()), type(RDD1.distinct().collect()))
    print("DistinctRDD2",  RDD2.distinct().collect())
    # DistinctRDD1 [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] <class 'pyspark.rdd.RDD'> <class 'pyspark.rdd.PipelinedRDD'> <class 'list'>
    # DistinctRDD2 [6, 7, 8, 9, 10]

    print("UnionRDD", RDD1.distinct().union(RDD2.distinct()).collect())
    # UnionRDD [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 6, 7, 8, 9, 10]

    print("IntersectionRDD", RDD1.intersection(RDD2).collect())
    # IntersectionRDD [6, 7, 8, 9, 10]
    print("IntersectionDistinctRDD", RDD1.intersection(RDD2).distinct().collect())
    # IntersectionDistinctRDD [6, 7, 8, 9, 10]

    print("SubstractRDD", RDD1.subtract(RDD2).collect())
    # SubstractRDD [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
    print("SubstractDistinctRDD", RDD1.subtract(RDD2).distinct().collect())
    # SubstractDistinctRDD [1, 2, 3, 4, 5]

    print("ReduceRDD", RDD1.reduce(lambda x, y: x + y))
    # ReduceRDD 70
    print("ReduceDistinctRDD", RDD1.distinct().reduce(lambda x, y: x + y))
    # ReduceDistinctRDD 55

    RDD3 = spark.sparkContext.parallelize(['gaurav', 'saurabh', 'ashish', 'ricky'])
    print(RDD3.filter(lambda x: 'a' in x).collect())
    # ['gaurav', 'saurabh', 'ashish']
    print("sorting RDD3", RDD3.sortBy(lambda x: x, ascending= True).collect())
    print("RDD3 No Of Partitions:", RDD3.getNumPartitions())
    # sorting RDD3 ['ashish', 'gaurav', 'ricky', 'saurabh']
    # RDD3 No Of Partitions: 12

    # Grouping and aggregating on RDD
    RDD4 = spark.sparkContext.parallelize([('a', 1), ('b', 2), ('a', 3), ('b', 4)])
    for k, v in  RDD4.groupBy(lambda x: x[0]).collect():
        print(k, ([i[1] for i in list(v)]))

    # Zip operation on RDD
    RDD5 = spark.sparkContext.parallelize([1, 2, 3])
    RDD6 = spark.sparkContext.parallelize(['a', 'b', 'c'])
    print("Zipped RDD:", RDD5.zip(RDD6).collect())
    # Zipped RDD: [(1, 'a'), (2, 'b'), (3, 'c')]

    # repartitioning RDD
    RDD7 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    RDD8 = RDD7.repartition(10)
    print("RDD7 Partitions:", RDD7.getNumPartitions())
    print("RDD8 Partitions:", RDD8.getNumPartitions())
    print("RDD7 Data:", RDD7.foreachPartition(lambda x: print(list(x))))
    print("RDD8 Data:", RDD8.foreachPartition(lambda x: print(list(x))))
    # RDD7 Partitions: 12
    # RDD8 Partitions: 10
    # RDD7 Data: None
    # RDD8 Data: None

    # Coalesce operation on RDD
    RDD9 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    RDD10 = RDD9.coalesce(1)
    print("RDD9 Partitions:", RDD9.getNumPartitions())
    print("RDD10 Partitions:", RDD10.getNumPartitions())
    print("RDD9 Data:", RDD9.foreachPartition(lambda x: print(list(x))))
    print("RDD10 Data:", RDD10.foreachPartition(lambda x: print(list(x))))
    # RDD9 Partitions: 12
    # RDD10 Partitions: 1
    # RDD9 Data: None
    # RDD10 Data: None


    # Mathematical operations on RDD
    RDD11 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    print("Sum of RDD11:", RDD11.sum())
    print("Mean of RDD11:", RDD11.mean())
    print("Max of RDD11:", RDD11.max()) 
    print("Min of RDD11:", RDD11.min())
    print("Count of RDD11:", RDD11.count())
    print("Variance of RDD11:", RDD11.variance())
    print("Standard Deviation of RDD11:", RDD11.stdev())


    # will create the same no of files as the no of partitions
    # saveAsTextFile will work only with RDDs, not with DataFrames
    RDD9 = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    print("RDD9", RDD9.collect())
    print("RDD9 partitions", RDD9.getNumPartitions())
    # RDD9 [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    # RDD9 partitions 12
    RDD9.saveAsTextFile("data/output_rdd9/output")
    !hadoop fs -ls data/output_rdd9/output/
    # Found 13 items
    # -rw-r--r--   3 itv020752 supergroup          0 2026-01-05 05:04 data/output_rdd9/output/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup          0 2026-01-05 05:04 data/output_rdd9/output/part-00000
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00001
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00002
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00003
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00004
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00005
    # -rw-r--r--   3 itv020752 supergroup          0 2026-01-05 05:04 data/output_rdd9/output/part-00006
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00007
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00008
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00009
    # -rw-r--r--   3 itv020752 supergroup          2 2026-01-05 05:04 data/output_rdd9/output/part-00010
    # -rw-r--r--   3 itv020752 supergroup          3 2026-01-05 05:04 data/output_rdd9/output/part-00011

    #caching in RDD
    RDD8.cache()
    print("RDD8 cached")
    print("RDD8 Data after caching:", RDD8.collect())
    print(RDD8.getStorageLevel(), "RDD8 is cached in memory", RDD8.is_cached, RDD8.getNumPartitions())
    # RDD8 cached
    # RDD8 Data after caching: [8, 2, 3, 6, 1, 4, 9, 7, 10, 5]
    # Memory Serialized 1x Replicated RDD8 is cached in memory True 10


    #persisting in RDD


    # StorageLevel.MEMORY_ONLY_2 'Memory Serialized 2x Replicated'
    #StorageLevel.MEMORY_AND_DISK_2 'Memory and Disk Serialized 2x Replicated'
    RDD8.unpersist() # to remove the previous cache
    RDD8.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
    print("RDD8 persisted")
    print("RDD8 Data after persisting:", RDD8.collect())
    print(RDD8.getStorageLevel(), "RDD8 is persisted in memory and disk", RDD8.is_cached, RDD8.getNumPartitions())
    # RDD8 persisted
    # RDD8 Data after persisting: [8, 2, 3, 6, 1, 4, 9, 7, 10, 5]
    # Disk Memory Serialized 2x Replicated RDD8 is persisted in memory and disk True 10

    time.sleep(3600)  # Keep the Spark session alive for an hour
    spark.stop()
    print("Spark session stopped")
