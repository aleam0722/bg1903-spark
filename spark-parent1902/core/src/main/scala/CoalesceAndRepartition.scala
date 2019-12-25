import org.apache.spark.{SparkConf, SparkContext}

object CoalesceAndRepartition {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CoalesceAndRepartition"))
  def main(args: Array[String]): Unit = {
    coalesceAndRepartiton(sc)
  }
  def coalesceAndRepartiton(sc :SparkContext): Unit = {
    val lis = 1 to 100
//    val lisRdd = sc.parallelize(lis)
//    val parNum = lisRdd.getNumPartitions
//    println(parNum)
    val lisRdd = sc.parallelize(lis).coalesce(10, shuffle = true)
    /*coalesce 一般用于减少分区，如果需要增加分区则需要增加第二个参数，shuffle=true*/
    /*增加分区还有一种简写的方法 repartition（）*/
    val parNum2 = lisRdd.getNumPartitions
    println(parNum2)
  }
}
