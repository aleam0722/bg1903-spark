package day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyOp {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ReduceByKeyOp"))
  def main(args: Array[String]): Unit = {
    recudeTest(sc)
  }
  def recudeTest(sc: SparkContext): Unit = {
    val words = List(
      "hello you",
      "hello me",
      "hello everyone"
    )

    val liens = sc.parallelize(words)
    //*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/**/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/
    val ret1:RDD[String] = liens.flatMap(lin => lin.split("\\s+"))
    val ret2:RDD[(String,Int)] = ret1.map(word =>{
      (word,1)
    })
//    val ret3:RDD[(String,Iterable[Int])] = ret2.groupByKey()
    /*可以理解为在groupbykey的基础上对Iteratabl【V】的部分进行了迭代计算*/
    val ret3:RDD[(String,Int)] = ret2.reduceByKey((num1, num2) => {
      num1+num2
    })
    ret3.foreach(println)
  }
}
