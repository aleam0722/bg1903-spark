import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark_Streaming {
  /**
   * 构建sparkStreaming需要两个参数
   *   -sparkConf
   *   -batchDuration
   */
  val sparkConf = new SparkConf().setAppName("Spark_Streaming").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf,Seconds(1))
  def main(args: Array[String]): Unit = {

  }
}
