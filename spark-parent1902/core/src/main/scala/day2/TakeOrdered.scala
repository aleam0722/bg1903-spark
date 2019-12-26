package day2

import org.apache.spark.{SparkConf, SparkContext}

object TakeOrdered {
  val list = List(
    Worker(1, "吴轩宇", 19, 168),
    Worker(2, "彭国宏", 18, 175),
    Worker(3, "随国强", 18, 176),
    Worker(4, "闫  磊", 20, 180),
    Worker(5, "王静轶", 18, 168.5)
  )
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("TakeOrdered"))
  def main(args: Array[String]): Unit = {
    takeOrdered(sc)
  }
  def takeOrdered(sc: SparkContext): Unit ={
    val Workers = sc.parallelize(list)
    val ret = Workers.takeOrdered(3) (
      new Ordering[Worker] {
        override def compare(x: Worker, y: Worker): Int = {
          var ret = y.height.compareTo(x.height)
          if (ret == 0){
              ret = x.age.compareTo(y.age)
          }
          ret
        }
      }
    )

    ret.foreach(println)
  }
}
case class Worker(id:Int, name:String, age:Int, height:Double)