package streamingkafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CheckpointKafkaDirect {

  val sparkConf = new SparkConf().setAppName("CheckpointKafkaDirect").setMaster("local[*]")
  val duration = Seconds(2)
  val checkpointPath = "file:///f:/data/spark/chkpoint"


  def creatingFunc():StreamingContext = {
    val ssc = new StreamingContext(sparkConf,duration)
    ssc.checkpoint(checkpointPath)
    val kafkaParam = Map[String,String](
      "bootstrap.servers" -> "bigdata1:9092,bigdata2:9092,bigdata3:9092",
      "auto.offset.reset" -> "largest"
    )

    val topics = Set[String](
      "hadoop"
    )

    val message:InputDStream[(String,String)] = KafkaUtils
      .createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,topics)

    message.print()
    message.foreachRDD(rdd => {
      val offsetrdds= rdd.asInstanceOf[HasOffsetRanges]
      val offsetranges = offsetrdds.offsetRanges
      for ( offsetrange <- offsetranges ){
        val offsetFrom = offsetrange.fromOffset
        val partitons = offsetrange.partition
        println(s"offsetFrom=${offsetFrom},partition=${}")
      }
    })
    ssc
  }


  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointPath,creatingFunc)
    ssc.start()
    ssc.awaitTermination()
  }
}
