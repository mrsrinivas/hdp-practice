from pyspark.sql import SparkSession
from pyspark.sql import Row

def getFirstAndLastLines(filePairRdd):
    #Here filename holds abs path. Feel free to substring as per your needs
    return Row(filePairRdd[0], filePairRdd[1].split("\\n")[0], filePairRdd[1].split("\\n")[-1])


spark = SparkSession \
    .builder \
    .appName("Read CSVs") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

records = spark.sparkContext\
    .wholeTextFiles("/some-hdfs-path/level-*_var-*/*.csv")\
    .map(lambda x : Row(x[0], x[1].split("\\n")[0], x[1].split("\\n")[-1]))\
    .collect()

for record in records:
    print(record)



