import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("crimes").getOrCreate()
var dfs = new Array[org.apache.spark.sql.DataFrame](11)
for(year <- 2006 to 2016){
  var path = "/FileStore/tables/5pgci1h31491830395383/crime_incidents_" + year + "_CSV.csv"
  dfs(year - 2006) = spark.read.format("csv").option("header", "true").load(path)
  dfs(year - 2006).createOrReplaceTempView("crimes" + year)
  dfs(year - 2006) = spark.sql("select REPORTDATETIME, METHOD from crimes" + year)
}
var df = dfs(0)
for(year <- dfs.slice(1, 11)){
  df = df.union(year)
}
var year = {time: String => time.split(" ")(0).split("/")(2)}
var yearudf = udf(year)
var annual = df.withColumn("year", yearudf($"REPORTDATETIME"))
annual.createOrReplaceTempView("crimes")

spark.sql("select year, count(*) as total, sum(case when METHOD = 'GUN' then 1 else 0 end) as gun, sum(case when METHOD = 'GUN' then 1 else 0 end) * 1.0 / count(*) as percentage from crimes group by year").orderBy($"year").show()