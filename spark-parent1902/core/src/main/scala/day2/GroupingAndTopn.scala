package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/*字段分别为科目，姓名，成绩。要求：求出每个科目成绩排名前3的学生信息。*/
object GroupingAndTopn {
  val list = List(
      Stu("english","ww",56),
      Stu("chinese","zs",90),
      Stu("chinese","zl",76),
      Stu("chinese","ls",91),
      Stu("english","zq",88),
      Stu("english","zq",88),
      Stu("english","zq",88),
      Stu("english","zq",88),
      Stu("english","zq",88),
      Stu("english","zq",88),
      Stu("english","zq",88),
      Stu("english","zq",88)
  )
  val sc = new SparkContext(new SparkConf().setAppName("GroupingAndTopn").setMaster("local[*]"))
  def main(args: Array[String]): Unit = {
    groupAndTopn(sc)
  }
  def groupAndTopn(sc: SparkContext): Unit = {
    val liens = sc.parallelize(list)
    /*将信息根据科目进行归纳*/
    val students = liens.map(stu => (stu.course,stu))
    /*将所有的信息根据科目进行归并，归并后的结果为(String, ArrayBuffer[Stu])*/
    val groupbyScour:RDD[(String, ArrayBuffer[Stu])] = students.combineByKey(createCombiner, mergeValue, mergeCombiners)

    val sortIntheGroup  = groupbyScour.map{case (course , infos) => {
      var topN = mutable.TreeSet[Stu]()(
        new Ordering[Stu] {
          override def compare(x: Stu, y: Stu): Int = y.score.compareTo(x.score)
        }
      )
      for(info <- infos){
        topN.add(info)
      }
      (course,topN.take(3))
    }}
    sortIntheGroup.foreach(println)
  }












  def createCombiner (stu: Stu): ArrayBuffer[Stu] ={
    val studentsCombine = ArrayBuffer[Stu]()
    studentsCombine.append(stu)
    studentsCombine
  }
  def mergeValue(combinner: ArrayBuffer[Stu], stu:Stu) : ArrayBuffer[Stu] ={
    combinner.append(stu)
    combinner
  }
  def mergeCombiners(combinner1: ArrayBuffer[Stu], combinner2: ArrayBuffer[Stu]): ArrayBuffer[Stu] = {
    combinner1 ++ combinner2
  }
}

case class Stu(course:String, name:String, score:Int)
