package day2

import org.apache.spark.{SparkConf, SparkContext}

object sortByKeyOp {
  val sc = new SparkContext(new SparkConf().setAppName("sortByKeyOp").setMaster("local[*]"))
  def main(args: Array[String]): Unit = {
    sortByKeyOp(sc)
  }
  def sortByKeyOp(sc: SparkContext): Unit = {
    val list = List(
        Student(1, "吴轩宇", 19, 168),
        Student(2, "彭国宏", 18, 175),
        Student(3, "随国强", 18, 176),
        Student(4, "闫  磊", 20, 180),
        Student(5, "王静轶", 18, 168.5)
    )

    val studentDRR = sc.parallelize(list)
    val ret = studentDRR.map(stu => (stu.height,stu)).sortByKey()
    ret.foreach(println)

    /**result----------------->
     * (168.5,Student(5,王静轶,18,168.5))
     * (180.0,Student(4,闫  磊,20,180.0))
     * (175.0,Student(2,彭国宏,18,175.0))
     * (176.0,Student(3,随国强,18,176.0))
     */
  }
}

case class Student(id: Int, name: String, age: Int, height:Double)
