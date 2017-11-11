import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
var path = "/FileStore/tables/uma0r6411491823202899/crime_incidents_2013_CSV.csv"
val spark = SparkSession.builder().appName("crimes").getOrCreate()
var df = spark.read.format("csv").option("header", "true").load(path)
df.createOrReplaceTempView("crimes")
var month = {time: String => time.split("/")(0)}
var monthudf = udf(month)
df.withColumn("month", monthudf($"REPORTDATETIME"))
var hour = {time: String => time.split(" ")(1).split(":")(0)}
var hourudf = udf(hour)
df.withColumn("hour", hourudf($"REPORTDATETIME")).groupBy($"hour").agg(count($"hour")).show()