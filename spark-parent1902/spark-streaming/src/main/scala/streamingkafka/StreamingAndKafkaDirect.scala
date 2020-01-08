package streamingkafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingAndKafkaDirect {
  val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("StreamingAndKafkaDirect"),Seconds(1))
  def main(args: Array[String]): Unit = {
    streamingKafkaDirect(ssc)
  }
  def streamingKafkaDirect(ssc: StreamingContext): Unit = {
    val kafkaParams = Map[String,String](
      "group.id"->"chengxubin",
      "bootstrap.servers"->"bigdata1:9092,bigdata:9092,bigdata3:9092",
      "auto.offset.reset"-> "smallest"
    )
    val topics = "hadoop".split(",").toSet
    /**
     * 将sparkStreaming和kafka整合，就是讲sparkStreaming的程序当做kafka消息的消费者
     * Q从哪读：
     *   A从输入的流中读取--->InputStream中
     *     Q怎么读：
     *     A需要：ssc（sparkStreaming的编程入口）
     *            kafkaParams（kafka集群的配置信息）
     *            topics（所读取信息所属的主题信息）
     */
    val massage:InputDStream[(String,String)]=
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
        ssc,kafkaParams,topics
      )

    /*-------------------------------------------------------------------------------------------------*/
    massage.foreachRDD((rdd) => {
      val offsetRdd = rdd.asInstanceOf[HasOffsetRanges]
      val offsetRanges = offsetRdd.offsetRanges
      for(offsetRange <- offsetRanges){
        val topic = offsetRange.topic
        val partition = offsetRange.partition
        val fromoffset = offsetRange.fromOffset
        println(s"${topic},${partition},${fromoffset}")
      }
    })
    massage.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
