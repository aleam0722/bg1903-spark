import org.apache.spark.{SparkConf, SparkContext}

object WordCountInScala {
  def main(args: Array[String]): Unit = {
    /*创建编程入口SparkContext*/
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountInScala")
    val sparkContext = new SparkContext(sparkConf)


    /*获取数据并操作*/
    val lines = sparkContext.textFile("E:/word.txt")
    val words = lines.flatMap(_.split(","))
    val kv = words.map(word => (word, 1))
    val result = kv.reduceByKey(_ + _)
    result.foreach(println)
  }
}
