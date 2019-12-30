import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object DynamicWay {
  val spark = SparkSession.builder().master("local[*]").appName("DynamicWay").getOrCreate()
  def main(args: Array[String]): Unit = {
    dynamicWay(spark)
  }
  def dynamicWay(spark: SparkSession):Unit = {
    val row = spark.sparkContext.parallelize(
      List(
          Row(1, "李伟", 1, 180.0),
          Row(2, "汪松伟", 2, 179.0),
          Row(3, "常洪浩", 1, 183.0),
          Row(4, "麻宁娜", 0, 168.0)
      )
    )

    val schema = types.StructType(
      List(
          StructField("id", DataTypes.IntegerType, false),
          StructField("name", DataTypes.StringType, false),
          StructField("gender", DataTypes.IntegerType, false),
          StructField("height", DataTypes.DoubleType, false)
      )
    )
    val dataFrame = spark.createDataFrame(row,schema).show()
    spark.stop()
  }
}
