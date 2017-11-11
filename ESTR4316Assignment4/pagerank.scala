import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner

object pagerank{
    def main(args: Array[String]) = {
        val sc = new SparkContext()
        var raw = sc.textFile("web-Google.txt")
        val link = raw.map({line =>
            line.split("\t") match {
                case Array(src: String, dst: String) => (src.toInt, dst.toInt)
            }
        }).groupByKey()
        val links = link.partitionBy(new RangePartitioner(4, link)).persist()
        var ranks = links.map({pair => (pair._1, 1.0)})
        for (i <- 1 to 100){
            ranks = links.join(ranks).flatMap({
                case (src, (dsts, rank)) => dsts.map(dst => (dst, rank / dsts.size))
            }).reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
        }
        ranks.saveAsTextFile("output")
    }
}
