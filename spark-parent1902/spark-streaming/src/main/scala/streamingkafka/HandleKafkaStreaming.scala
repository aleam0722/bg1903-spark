package streamingkafka

import java.io.InputStream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HandleKafkaStreaming {

  val ssc = new StreamingContext(
    new SparkConf().setMaster("local[*]").setAppName("HandleKafkaStreaming"),
    Seconds(2)
  )

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String,String](
      "bootstrap.servers" -> "bigdata1:9092,bigdat2:9092,bigdata3:9092",
      "group.id" -> "chengxubin",
      "auto.offset.reset" -> "smallest"
    )

    val topics = Set[String](
      "szxg-exam"
    )

//    val message:InputDStream[(String,String)] = KafkaUtils.
//      createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    val message = createMessage(ssc, kafkaParams,topics)

  }

  def createMessage(ssc:StreamingContext, kafkaParams:Map[String,String], topics:Set[String]): InputDStream[(String,String)] ={

    var message:InputDStream[(String,String)] = null
    /*如果没有读到offset信息，则新建*/

    val fromOffSet:Map[TopicAndPartition,Long] = getFromOffset()

    if (fromOffSet.isEmpty)  {
     message = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    }else{
      val messageHandler = (messageAndMetadata: MessageAndMetadata[String,String]) => (messageAndMetadata.key(),messageAndMetadata.message())
      message = KafkaUtils.createDirectStream(ssc,kafkaParams,fromOffSet,messageHandler)
    }

    message
  }

  def getFromOffset():Map[TopicAndPartition,Long] ={
    null
  }

}
