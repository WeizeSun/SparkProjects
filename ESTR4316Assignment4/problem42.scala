import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.Pipeline

//import scala.util.Sorting.stableSort

val path = "/FileStore/tables/zte6aeyl1492852492139/ratings.csv"
var data = sc.textFile(path).mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}

val ratings = data.map(_.split(',') match {
  case Array(user, item, rate, time) => Rating(user.toInt, item.toInt, rate.toFloat)
}).toDF("user", "item", "rating")

val als = new ALS().setMaxIter(50).setUserCol("user").setItemCol("item").setRatingCol("rating")
val pipeline = new Pipeline().setStages(Array(als))

val paramGrid = new ParamGridBuilder().addGrid(als.rank, Array(25, 50, 100)).addGrid(als.regParam, Array(0.001, 0.01, 0.1)).build()

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)

val cvModel = cv.fit(ratings).bestModel

val predictions = cvModel.transform(ratings)

val MSE = evaluator.evaluate(predictions)
println("Mean Squared Error = " + MSE)