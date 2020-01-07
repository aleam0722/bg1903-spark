package streamingkafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamAndKafa {
  val ssc = new StreamingContext(new SparkConf().setAppName("StreamAndKafa").setMaster("local[*]"),Seconds(1))
  def main(args: Array[String]): Unit = {
    readMessageFormKafka(ssc)
  }
  def readMessageFormKafka(ssc: StreamingContext): Unit = {
    /*streaming  和 kafka 整合的入口是 KafkaUtils*/
    val kafkaParams = Map[String,String](
      "group.id" -> "chengxubin",
      "zookeeper.connect" -> "bigdata1:2181,bigdata2:2181,bigdata3:2181/kafka"
    )
    val topics = Map[String,Int](
      "hadoop" -> 3
    )
    val storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    val message: ReceiverInputDStream[(String,String)]= KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams, topics, storageLevel)
    message.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
