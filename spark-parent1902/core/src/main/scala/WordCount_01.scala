import org.apache.spark.{SparkConf, SparkContext}

object WordCount_01 {
  val list = List("hello you","hello me","hello you")
  val sc = new SparkContext(new SparkConf().setAppName("wc").setMaster("local[*]"))
  def main(args: Array[String]): Unit = {
    wordCount_(sc)
  }
  def wordCount_(sc: SparkContext): Unit = {

    val words = sc.parallelize(list)

    val result = words.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)

    result.foreach(println)
  }
}
