import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
var path = "/FileStore/tables/uma0r6411491823202899/crime_incidents_2013_CSV.csv"
val spark = SparkSession.builder().appName("crimes").getOrCreate()
var df = spark.read.format("csv").option("header", "true").load(path)
df.createOrReplaceTempView("crimes")
var offenses = df.groupBy($"OFFENSE").agg(count($"OFFENSE"))
offenses.show()