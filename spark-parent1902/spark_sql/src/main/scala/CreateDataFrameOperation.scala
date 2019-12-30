import org.apache.spark.sql.SparkSession

object CreateDataFrameOperation {
  val spark = SparkSession.builder().appName("CreateDataFrameOperation").master("local[*]").getOrCreate()
  def main(args: Array[String]): Unit = {
    refletionWay(spark)
  }

  def refletionWay(spark: SparkSession):Unit = {
    import scala.collection.JavaConversions._
    val list = List(
      new Student(1, "王盛芃", 1, 19),
      new Student(2, "李金宝", 1, 49),
      new Student(3, "张海波", 1, 39),
      new Student(4, "张文悦", 0, 29)
    )
    val dataFrame = spark.createDataFrame(list,classOf[Student])
    dataFrame.show()
    spark.stop()
  }
}
