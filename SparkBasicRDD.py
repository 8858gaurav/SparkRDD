import pyspark # type: ignore
from pyspark.sql.functions import * # type: ignore
from pyspark.sql import SparkSession # type: ignore
import getpass
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