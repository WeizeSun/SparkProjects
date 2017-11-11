import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.math.Ordering

import twitter4j._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

System.setProperty("twitter4j.oauth.consumerKey", "GhRoCN3Lxwrk7EcAfWLMaejtj")
System.setProperty("twitter4j.oauth.consumerSecret", "9caL3vGsRnLCK5189mYhYV9xj02j097yy657BuntPGcSuZUCQA")
System.setProperty("twitter4j.oauth.accessToken", "834729843723956224-OYLn7R7WUe3zcGt2vylojc3ZHIB1LP0")
System.setProperty("twitter4j.oauth.accessTokenSecret", "kCT0UnauUSnIWnDkuCVAEGuLumpwYWdyfAWUNaB5m9dEo")

val outputDirectory = "/twitter"
val slideInterval = new Duration(10 * 60 * 1000)
val windowLength = new Duration(10 * 60 * 1000)
val timeoutJobLength = 21 * 60 * 1000

//dbutils.fs.rm(outputDirectory, true)

var newContextCreated = false
var num = 0

object SecondValueOrdering extends Ordering[(String, Int)]{
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2.compare(b._2)
  }
}

def creatingFunc(): StreamingContext = {
  val ssc = new StreamingContext(sc, slideInterval)
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth, Array("trump"), StorageLevel.MEMORY_ONLY_SER)
  
  val hashTagStream = twitterStream.map(_.getText.toLowerCase()).flatMap(_.split(" ")).filter(_.startsWith("#"))
  
  val windowedhashTagCountStream = hashTagStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)
  
  windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
    val topEndpoints = hashTagCountRDD.top(10)(SecondValueOrdering)
    //dbutils.fs.put(outputDirectory + "/top_hashtags_" + num, topEndpoints.mkString("\n"), true)
    println("------ TOP HASHTAGS For window " + num)
    println(topEndpoints.mkString("\n"))
    num = num + 1
  })
  
  newContextCreated = true
  ssc
}

@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

ssc.start()
ssc.awaitTerminationOrTimeout(timeoutJobLength)
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }