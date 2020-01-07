package streamingkafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamAndKafa {
  val ssc = new StreamingContext(new SparkConf().setAppName("StreamAndKafa").setMaster("local[*]"),Seconds(1))
  def main(args: Array[String]): Unit = {

  }
}
