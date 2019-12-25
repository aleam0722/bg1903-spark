import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionOp {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("MapPartitionOp"))
  def main(args: Array[String]): Unit = {
    mapPatitinOp(sc)

  }
  def mapPatitinOp(sc: SparkContext): Unit = {
    val lis = 1 to 20
    val lisDRR = sc.parallelize(lis)
    val parNum = lisDRR.getNumPartitions
    println(parNum)
    val ret = lisDRR.mapPartitions(partition =>{
      partition.map(num => {
        (num,1)
      })
    })
//    ret.foreach(println)
  }
}
