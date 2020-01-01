import org.apache.spark.sql.SparkSession

object CreateDataSet {
  val spark = SparkSession.builder().master("local[*]").appName("CreateDataSet").getOrCreate()
  def main(args: Array[String]): Unit = {
    createDataSet(spark)
  }

  def createDataSet(spark: SparkSession): Unit = {
    val list = List(
      new Studentz(1, "chengxubin", 1, 24),
      new Studentz(2, "wangyujiao", 0, 24)
    )
    import spark.implicits._
    val dataSet = spark.createDataset(list)
    dataSet.show()
    spark.stop()
  }
}
/*创建dataset必须使用case class 或者 基本数据类型*/
case class Studentz(id:Int, name:String, gender:Int, age:Int)