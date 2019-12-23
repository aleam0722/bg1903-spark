import org.apache.spark.{SparkConf, SparkContext}

object WordCountInScala {
  def main(args: Array[String]): Unit = {
    /*创建编程入口SparkContext*/
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountInScala")
    val sparkContext = new SparkContext(sparkConf)
    println(sparkContext)
  }
}
