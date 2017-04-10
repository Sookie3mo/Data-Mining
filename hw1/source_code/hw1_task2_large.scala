import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Task2Large{
	def main(args: Array[String]){
		val spark = SparkSession.builder().appName("Task2Large").getOrCreate()
		val df = spark.read.format("org.apache.spark.csv").option("header","true").csv("hw1/ratings_large.csv")
		val df2 = spark.read.format("org.apache.spark.csv").option("header","true").csv("hw1/tags_large.csv")
		val data1 = df.selectExpr("movieId","cast(rating as float)rating")
		val data2 = df2.select("movieId","tag")
		val data = data2.join(data1,data1.col("movieId").equalTo(data2.col("movieId")),"inner").select("tag","rating")
		val avg = data.groupBy("tag").avg("rating").sort(desc("tag"))
		avg.coalesce(1).withColumnRenamed("avg(rating)","rating_avg").write.format("csv").option("header","true").save("hw1/Yiming_Liu_result_task2_large.csv")
	}
}