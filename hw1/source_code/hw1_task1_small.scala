import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Task1{
	def main(args: Array[String]){
		
		val spark = SparkSession.builder().appName("Task1").getOrCreate()
		val df = spark.read.format("org.apache.spark.csv").option("header","true").csv("hw1/ratings.csv")
		val data = df.selectExpr("cast(movieId as int)movieId","cast(rating as float)rating")
		val avg = data.groupBy("movieId").avg("rating").sort("movieId")
		avg.coalesce(1).write.format("csv").option("header","true").save("hw1/Yiming_Liu_result_task1_small.csv") 
	}
}