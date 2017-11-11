import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

import scala.util.Sorting.stableSort

val path = "/FileStore/tables/zte6aeyl1492852492139/ratings.csv"
var data = sc.textFile(path)
val header = data.first()
data = data.filter(_ != header)

val ratings = data.map(_.split(',') match {
  case Array(user, item, rate, time) => Rating(user.toInt, item.toInt, rate.toDouble)
})
val usersProducts = ratings.map({
  case Rating(user, product, rate) => (user, product)
})
val model = ALS.train(ratings, 50, 10, 0.01)

var top10of1 = model.recommendProducts(1, 10)
println(top10of1.mkString("\n"))
var top10of1001 = model.recommendProducts(1001, 10)
println(top10of1001.mkString("\n"))
var top10of10001 = model.recommendProducts(10001, 10)
println(top10of10001.mkString("\n"))

val predictions = model.predict(usersProducts).map({
  case Rating(user, product, rate) => ((user, product), rate)
})
val ratesAndPreds = ratings.map({
  case Rating(user, product, rate) => ((user, product), rate)
}).join(predictions)
val MSE = ratesAndPreds.map({
  case ((user, product), (r1, r2)) =>
  val err = (r1 - r2)
  err * err
}).mean()
println("Mean Squared Error = " + MSE)

/*
val usersProducts = ratings.map{
  case Rating(user, item, rate) => (user, item)
}
val predictions = model.predict(usersProducts).filter{
  case Rating(user, item, rate) => user == 1 || user == 1001 || user == 10001
}.map({
  case Rating(user, item, rate) => (user, item, rate)
}).collect()

var top10of1 = predictions.filter({
  case (user, item, rate) => user == 1
})
var top10of1001 = predictions.filter({
  case (user, item, rate) => user == 1001
})
var top10of10001 = predictions.filter({
  case (user, item, rate) => user == 10001
})
stableSort(top10of1, (e1: (Int, Int, Double), e2: (Int, Int, Double)) => e1._3 > e2._3)
stableSort(top10of1001, (e1: (Int, Int, Double), e2: (Int, Int, Double)) => e1._3 > e2._3)
stableSort(top10of10001, (e1: (Int, Int, Double), e2: (Int, Int, Double)) => e1._3 > e2._3)
println(top10of1.slice(0, 10).mkString("\n"))
println(top10of1001.slice(0, 10).mkString("\n"))
println(top10of10001.slice(0, 10).mkString("\n"))
*/
/*
Rating(1,81898,5.687878283892449)
Rating(1,7113,5.385741769575463)
Rating(1,104390,5.280303633943207)
Rating(1,8411,5.263275272985508)
Rating(1,56779,5.147435536042586)
Rating(1,73529,5.048276901197707)
Rating(1,58992,5.040036677597246)
Rating(1,51380,5.005851330749676)
Rating(1,70465,5.005383698099093)
Rating(1,86753,4.971867691405928)
Rating(1001,62206,4.697677131165795)
Rating(1001,56779,4.642415567664176)
Rating(1001,120821,4.634067641426862)
Rating(1001,6914,4.571663025433713)
Rating(1001,81898,4.567333243011817)
Rating(1001,93404,4.532546944872343)
Rating(1001,87358,4.521592362154809)
Rating(1001,73529,4.497994198741096)
Rating(1001,60880,4.478768192277405)
Rating(1001,117907,4.466298913265879)
Rating(10001,49359,5.129878031951021)
Rating(10001,27216,5.125185298624874)
Rating(10001,120815,5.10506311575124)
Rating(10001,82848,5.001939915540135)
Rating(10001,68874,5.000027188069998)
Rating(10001,113064,4.971520669604868)
Rating(10001,7396,4.950809966841302)
Rating(10001,81424,4.9309011732516215)
Rating(10001,8785,4.930379630285323)
Rating(10001,90929,4.929598790559663)
*/

//MSE: Double = 0.3269363128290787