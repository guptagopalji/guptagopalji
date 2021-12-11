"""
author Gopal Gupta
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('gopal').getOrCreate()

sub = [("Gopal","math",90), \
    ("Rajesh","science",80), \
    ("Gopal","hindi",95), \
    ("Mayank","english",89) \
  ]
subColumns = ["name","sub_name","number"]
subDF = spark.createDataFrame(data=sub, schema = subColumns)
subDF.printSchema()
subDF.show(truncate=False)

dataCollect = subDF.collect()

print(dataCollect)

dataCollect2 = subDF.select("sub_name").collect()
print(dataCollect2)

for row in dataCollect:
    print(row['name'] + "," + row['sub_name'] + "," +str(row['number']))
