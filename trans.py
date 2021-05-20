from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("My PySpark code").getOrCreate()

 df = spark.read.options(header='true', inferSchema='true').parquet("file:///home/zhur/stations/*")
//df = spark.read.options(header='true', inferSchema='true').parquet("station/*")

df.printSchema()
df.createOrReplaceTempView("sales")

//df.write.parquet("result.parquet")
 df.write.parquet("file:///home/zhur/result.parquet")