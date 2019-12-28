package day3

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
/**
 * 自定分区
 * 数据中有不同的学科,将输出的一个学科生成一个文件
 */
object PartitionTest {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("PartitionTest"))

  def main(args: Array[String]): Unit = {
    partitionTest(sc)
  }
/*去除URL*/
  def partitionTest(sd: SparkContext):Unit ={
    val lines = sc.textFile("File:///e:/access.txt")
    val takeURL = lines.map{case (line) =>
        val URL = line.split("\\s+")(1)
      (URL,1)
    }
    /*获取各个URL的点击数*/
    val URLCount = takeURL.reduceByKey(_+_)
    /*根据URl中的关键字转换为学科字符串串--->http://   ${java}   .learn.com/java/javaee.shtml*/
    val subjectAndCount = URLCount.map{case (urlStr,count) =>
        val subject = new URL(urlStr).getHost
      (subject,count)
    }
    /*因为是根据学科进行分区，而分区分区存放在RDD--->subjectAndCount的key中，所以我们需要收集所有的key值*/
    val subjects = subjectAndCount.keys.distinct.collect
    /*创建一个自定义分区器，传入我们分区依据的字段，也就是我们的学科subject字段*/
    val partitioner = new SubjectPartitioner(subjects)
    /*进行分区*/
    val result = subjectAndCount.partitionBy(partitioner)
    /*最后输出到指定的文件目录下*/
    result.saveAsTextFile("File:///e:/output")
    subjectAndCount.foreach(println)
  }


}
