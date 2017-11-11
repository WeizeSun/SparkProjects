import org.apache.spark.sql.SparkSession
var path = "/FileStore/tables/uma0r6411491823202899/crime_incidents_2013_CSV.csv"
val spark = SparkSession.builder().appName("crimes").getOrCreate()
var df = spark.read.format("csv").option("header", "true").load(path)
df.createOrReplaceTempView("crimes")
val cromld = spark.sql("select CCN, REPORTDATETIME, OFFENSE, METHOD, LASTMODIFIEDDATE, DISTRICT from crimes where CCN is not null and REPORTDATETIME is not null and OFFENSE is not null and METHOD is not null and LASTMODIFIEDDATE is not null and DISTRICT is not null")
cromld.show()