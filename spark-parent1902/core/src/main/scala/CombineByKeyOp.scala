import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object CombineByKeyOp {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CombineByKeyOp"))
  def main(args: Array[String]): Unit = {
    combineByKeyOp(sc)
  }
  def combineByKeyOp(sc : SparkContext): Unit = {
    val stuList = List(
      "白普州,1904-bd-bj",
      "伍齐城,1904-bd-bj",
      "曹佳,1904-bd-sz",
      "刘文浪,1904-bd-wh",
      "姚远,1904-bd-bj",
      "匿名大哥,1904-bd-sz",
      "欧阳龙生,1904-bd-sz"
    )
    val lien = sc.parallelize(stuList)
    val stumap:RDD[(String,String)] = lien.map(lin => (lin.split(",")(1), lin))
    stumap.combineByKey(createCombiner,mergeValue,mergeCombiners).foreach(println)
  }
  def createCombiner(keys:String): ArrayBuffer[String] ={
    val combinerr = ArrayBuffer[String]()
    combinerr.append(keys)
    combinerr
  }
  def mergeValue(combienerr:ArrayBuffer[String], student:String): ArrayBuffer[String] = {
    combienerr.append(student)
    combienerr
  }
  def mergeCombiners(combinerr1: ArrayBuffer[String], combinerr2: ArrayBuffer[String]) = {
    combinerr1 ++ combinerr2
  }
  sc.stop()
}
