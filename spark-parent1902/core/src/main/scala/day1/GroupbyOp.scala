package day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupbyOp {
  val sc = new SparkContext(new SparkConf().setAppName("GroupbyOp").setMaster("local[*]"))
  def main(args: Array[String]): Unit = {
    GroupbyKeyOp(sc)
  }
  def GroupbyKeyOp(sc : SparkContext): Unit = {
    /*对不同班级的学生进行分组*/
    val stuList = List(
      "1,白普州,1,22,1904-bd-bj",
      "2,伍齐城,1,19,1904-bd-wh",
      "3,曹佳,0,27,1904-bd-sz",
      "4,姚远,1,27,1904-bd-bj",
      "5,匿名大哥,2,17,1904-bd-wh",
      "6,欧阳龙生,0,28,1904-bd-sz"
    )
    val stuinfos = sc.parallelize(stuList)
    val ret:RDD[(String,String)]= stuinfos
      .map(lin =>{
        val index = lin.lastIndexOf(",")
        val classname = lin.substring(index+1)
        val info = lin.substring(0,index)
        (classname,info)
      })

    val result = ret.groupByKey()
    result.foreach(println)
  }
}
