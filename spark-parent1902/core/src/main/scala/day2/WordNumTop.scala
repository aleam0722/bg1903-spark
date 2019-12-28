package day2

import org.apache.spark.{SparkConf, SparkContext}

object WordNumTop {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordNumTop"))
  def main(args: Array[String]): Unit = {
    wordNumTop(sc)
  }

  def wordNumTop(sc: SparkContext): Unit = {

    val liens = sc.textFile("File:///e:/dierti.txt")

    val linesAndWords = liens.map{case (line:String) =>
      val words = line.split("\\s+").size
      (words ,line)
    }.sortByKey(ascending = false)

    val result = linesAndWords.take(1)
    result.foreach(println)
  }
}
