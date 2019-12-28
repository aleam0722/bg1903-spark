
package day2
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ADCounter {
  val sc = new SparkContext(new SparkConf().setAppName("ADCounter").setMaster("local[*]"))
  val dataformat = new SimpleDateFormat("yyyy-MM-dd:HH")

  def main(args: Array[String]): Unit = {
    adCounter(sc)
  }


  /*1516609143867   6      7     64         16
    timestamp    proid   city   userid     adid

    proid    hour   adid   count
   */

  def adCounter(sc: SparkContext): Unit = {
    val liens = sc.textFile("File:///e:/Advert.txt")

    val proAndadInfo:RDD[(String,Int)] = liens.map{ case(line) =>
      val provinceid = line.split("\\s+")(1)
      val hour = timeStampToHour(line.split("\\s+")(0))
      val adid = line.split("\\s+")(4)
      (s"${provinceid}_${hour}_${adid}",1)
    }.reduceByKey(_+_)
    val result = proAndadInfo.map{case(pro_hour_adid,count) =>{
      val provinceid = pro_hour_adid.split("_")(0)
      val hour = pro_hour_adid.split("_")(1)
      val adid = pro_hour_adid.split("_")(2)
      (provinceid+"_"+hour,adid+"_"+count)
    }
    }
    val ret:RDD[(String,mutable.TreeSet[String])] = result.combineByKey(createCombiner,mergeValue,mergeCombiners)
    ret.foreach(println)
  }


  def createCombiner(value:String):mutable.TreeSet[String]={
    val treeSet = new mutable.TreeSet[String]()
    treeSet.add(value)
    treeSet
  }
  def mergeValue(combiner:mutable.TreeSet[String], value:String): mutable.TreeSet[String] ={
    combiner.add(value)

    if (combiner.size > 3){
      combiner.take(3)
    }else{
      combiner
    }
  }
  def mergeCombiners(combiner1:mutable.TreeSet[String],combinner2:mutable.TreeSet[String]):mutable.TreeSet[String] = {
    val ret = combiner1 ++ combinner2
    if (ret.size > 3){
      ret.take(3)
    }else{
      ret
    }
  }

  def timeStampToHour(timeStamp: String):String ={
    val date = new Date(timeStamp.toLong)
    dataformat.format(date).toString
  }
}
