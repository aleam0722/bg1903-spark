import org.apache.spark.sql.SparkSession

object SparkSqlOp {
  /*sparkSession的构建*/
  val spark = SparkSession.builder().master("local[*]").appName("SparkSqlOp").getOrCreate()
  def main(args: Array[String]): Unit = {
    loadData(spark)
  }
  def loadData(spark:SparkSession):Unit = {
    /*加载一个json格式的数据*/
    val dataFrame = spark.read.json("File:///E:/sparkData/sql/account.json")
    dataFrame.printSchema()

    /**
     * result-------->
     * root
     * |-- account_number: long (nullable = true)
     * |-- address: string (nullable = true)
     * |-- age: long (nullable = true)
     * |-- balance: long (nullable = true)
     * |-- city: string (nullable = true)
     * |-- email: string (nullable = true)
     * |-- employer: string (nullable = true)
     * |-- firstname: string (nullable = true)
     * |-- gender: string (nullable = true)
     * |-- lastname: string (nullable = true)
     * |-- state: string (nullable = true)
     */
  }

}
