package streamingkafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingAndKafkaDirect {
  val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("StreamingAndKafkaDirect"),Seconds(1))
  def main(args: Array[String]): Unit = {

  }
  def streamingKafkaDirect(ssc: StreamingContext): Unit = {
    val kafkaParams = Map[String,String](
      "group.id"->"chengxubin",
      "bootstrap-server"->"bigdata1:9092,bigdata:9092,bigdata3:9092",
      "auto.offset.reset"-> "largest"
    )
    val topics = "hadoop".split(",").toSet
    val massage:InputDStream[(String,String)]=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder]( ssc,kafkaParams,topics)
    massage.foreachRDD()
    massage.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
