from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("My PySpark code").getOrCreate()

df = spark.read.options(header='true', inferSchema='true').parquet("station/*")

df.printSchema()
df.createOrReplaceTempView("sales")

df.write.parquet("result.parquet")