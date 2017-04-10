/**

  * inf553 homework3 task1 
  */

import java.io.PrintWriter
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

object task1 {
  def main(args: Array[String]) {
    //Spark configuration
    val conf = new SparkConf().setAppName("CF").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Prepare training/testing data
    //args(0) : input/ratings_small.csv or input/ratings_20m.csv
    val data = sc.textFile(args(0))
    //val data = sc.textFile("input/ratings_small.csv")
    val header = data.first()
    val data0 = data.filter(rows => rows != header)
    val alldata = data0.map(_.split(',') match { case Array(user,item,rate,time) =>
      ((user.toInt, item.toInt), rate.toDouble)})
    //args(1): input/testing_small.csv  or input/testing_20m.csv
    val data1 = sc.textFile(args(1))
    //val data1 = sc.textFile("input/testing_small.csv")
    val header1 = data1.first()
    val data2 = data1.filter( rows => rows != header1)

    val testdata = data2.map(_.split(',') match { case Array(user, item) =>
      ((user.toInt, item.toInt), 0.0)})

    val traindata = alldata.subtractByKey(testdata)

    val trainings = traindata.map(traindata => traindata match { case ((user, item), rate) => Rating(user, item, rate)})

    // Build the recommendation model using ALS
    val rank = 5
    val numIterations = 10
    val model = ALS.train(trainings, rank, numIterations, 0.05)


    // Evaluate the model on rating data
    val testings = testdata.map { case ((user, product), rate) => (user, product)}
    val predictions = model.predict(testings).map { case Rating(user, product, rate) => ((user, product), rate)}
    //predictions.take(10).foreach(println)

    val part1 = alldata.join(predictions)
    //part1.take(20).foreach(println)
    val testWithAnswer = alldata.subtractByKey(traindata)
    val leftPredictions = testWithAnswer.subtractByKey(part1)

    //all train data get mean rating for every user
    //val rdd = traindata.map{case ((user, item), rate) => (user, rate)}.groupByKey()

//    def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
//      num.toDouble( ts.sum ) / ts.size
//    }
    //val avgs = rdd.map(x => (x._1, average(x._2)))

    //avgs : (user, mean rate)
    //val part2 = leftPredictions.map{ case ((user,product),rate) => (user,(product,3.65))}

    val part22 = leftPredictions.map{case ((user,product),rate) => ((user,product),(rate,3.65))}
    //print(part22.count())
    val allPredictions = part22.union(part1)
    //print(part1.count())
    //print(allPredictions.count())
    val predictionsAndLabels = allPredictions.map { case ((user, product), (actual, predicted)) => (predicted, actual)}

    val predictedRatings = allPredictions.map{ case ((user, product), (actual,rate)) =>((user, product), (rate,math.abs(actual-rate)))}


    val r1 = predictedRatings.filter(x => x._2._2 >= 0 && x._2._2 < 1).collect()
    val r2 = predictedRatings.filter(x => x._2._2 >= 1 && x._2._2 < 2).collect()
    val r3 = predictedRatings.filter(x => x._2._2 >= 2 && x._2._2 < 3).collect()
    val r4 = predictedRatings.filter(x => x._2._2 >= 3 && x._2._2 < 4).collect()
    val r5 = predictedRatings.filter(x => x._2._2 >= 4).collect()
    val result = (r1 ++ r2 ++ r3 ++ r4 ++ r5).sorted
    val c1 = r1.length
    val c2 = r2.length
    val c3 = r3.length
    val c4 = r4.length
    val c5 = r5.length

    val file = new PrintWriter(new File("Yiming_Liu_result_task1.txt"))
    file.write("UserId,MovieId,Pred_rating\n")
    result.foreach{x => file.write(x._1._1.toString + "," + x._1._2.toString + "," + x._2._1.toString+"\n")}
    file.close()
    // Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
    println(s">=0 and <1: $c1")
    println(s">=1 and <2: $c2")
    println(s">=2 and <3: $c3")
    println(s">=3 and <4: $c4")
    println(s">=4: $c5")
    println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")

    sc.stop()


  }
}



