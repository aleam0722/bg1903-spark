package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupAndTopN_II {
  val list = List(
    Ter("english","ww",56),
    Ter("chinese","zs",90),
    Ter("chinese","zl",76),
    Ter("chinese","ls",91),
    Ter("english","zq",88),
    Ter("english","zq",88),
    Ter("english","zq",88),
    Ter("english","zq",88),
    Ter("english","zq",88),
    Ter("english","zq",88),
    Ter("english","zq",88),
    Ter("english","zq",88)
  )
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("GroupAndTopN_II"))
  def main(args: Array[String]): Unit = {
    groupAndTopN_II(sc)
  }

  def groupAndTopN_II(sc: SparkContext): Unit ={
    val ters = sc.parallelize(list)
    val ters2map = ters.map(ter => (ter.scour,ter))
    
  }
}

case class Ter(scour:String, name:String, score:Int)