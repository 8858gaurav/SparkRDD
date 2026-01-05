import pyspark

from pyspark.sql.functions import * 
from pyspark.sql import SparkSession 
import getpass, time
username = getpass.getuser()
print(username)

spark = SparkSession \
            .builder \
            .appName("debu application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
            .enableHiveSupport() \
            .config("spark.driver.bindAddress","localhost") \
            .config("spark.driver.host", '192.168.1.3') \
            .config('spark.driver.port', "4041") \
            .config("spark.ui.port","4040") \
            .master("local[*]") \
            .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

for k,v in spark.sparkContext.getConf().getAll():
    print("The value of {0}, and it's values is {1}".format(k, v))
# The value of spark.eventLog.enabled, and it's values is true
# The value of spark.sql.repl.eagerEval.enabled, and it's values is true
# The value of spark.eventLog.dir, and it's values is hdfs:///spark-logs
# The value of spark.dynamicAllocation.maxExecutors, and it's values is 10
# The value of spark.ui.port, and it's values is 4040
# The value of spark.sql.shuffle.partitions, and it's values is 3
# The value of spark.sql.warehouse.dir, and it's values is /user/itv020752/warehouse
# The value of spark.yarn.historyServer.address, and it's values is m02.itversity.com:18080
# The value of spark.app.name, and it's values is debu application
# The value of spark.history.provider, and it's values is org.apache.spark.deploy.history.FsHistoryProvider
# The value of spark.driver.bindAddress, and it's values is localhost
# The value of spark.serializer.objectStreamReset, and it's values is 100
# The value of spark.master, and it's values is local[*]
# The value of spark.history.fs.logDirectory, and it's values is hdfs:///spark-logs
# The value of spark.submit.deployMode, and it's values is client
# The value of spark.history.fs.update.interval, and it's values is 10s
# The value of spark.driver.extraJavaOptions, and it's values is -Dderby.system.home=/tmp/derby/
# The value of spark.executor.extraLibraryPath, and it's values is /opt/hadoop/lib/native
# The value of spark.driver.port, and it's values is 4044
# The value of spark.app.id, and it's values is local-1767610500959
# The value of spark.history.ui.port, and it's values is 18080
# The value of spark.shuffle.service.enabled, and it's values is true
# The value of spark.app.startTime, and it's values is 1767610499552
# The value of spark.dynamicAllocation.minExecutors, and it's values is 2
# The value of spark.executor.id, and it's values is driver
# The value of spark.history.fs.cleaner.enabled, and it's values is true
# The value of spark.yarn.jars, and it's values is hdfs:///spark-jars/*.jar
# The value of spark.sql.catalogImplementation, and it's values is hive
# The value of spark.rdd.compress, and it's values is True
# The value of spark.submit.pyFiles, and it's values is 
# The value of spark.dynamicAllocation.enabled, and it's values is true
# The value of spark.driver.host, and it's values is 192.168.1.3
# The value of spark.ui.showConsoleProgress, and it's values is true

print(spark.sparkContext.uiWebUrl.split(":")[-1])
# spark web UI url for the current spark session.
# 4045

print(spark.sparkContext.uiWebUrl.split(":"))
# ['http', '//192.168.1.3', '4045']

# the difference between spark.driver.bindAddress, and spark.driver.host is:
# spark.driver.bindAddress: where the driver binds(listen) for conections, used by the executors to connect into the drivers.
# spark.driver.bindAddress=0.0.0.0 ; driver listens on all interfaces
# if it's wrong then executot can't reach driverrs. 

# spark.driver.host : spark tells executors, connect to the driver at this address.
# spark.driver.host  = driver-service.my-cluster.local
# if it's wrong, executors fail with connection refused/timeout

# spark.driver.bindAddress=0.0.0.0
# spark.driver.host=actual-hostname-or-ip

# spark.driver.port: executor connect to this port, if not set, spark picks a random free port.
# spark.ui.port: port of spark web ui, no efect on executor communication

# propety                      Used By
# spark.driver.bindAddress     OS / N/w
# spark.driver.host            Executors
# spark.driver.port            Executors
# spark.ui.port                Browser
