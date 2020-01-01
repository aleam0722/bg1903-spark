import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object DynamicWay {
  val spark = SparkSession.builder().master("local[*]").appName("DynamicWay").getOrCreate()
  def main(args: Array[String]): Unit = {
    dynamicWay(spark)
  }
  def dynamicWay(spark: SparkSession):Unit = {

    /*这里是行，也就是我们传递一条一条的数据，使用row对象进行封装*/
    val row = spark.sparkContext.parallelize(
      List(
          Row(1, "李伟", 1, 180.0),
          Row(2, "汪松伟", 2, 179.0),
          Row(3, "常洪浩", 1, 183.0),
          Row(4, "麻宁娜", 0, 168.0)
      )
    )
/*这里是表头信息，也就是我们每行每个数据所对应的的字段信息，这里采用StrutType对象进行封装，这个StrutType对象的构造器
  要求我们传递StructFile（结构化字段）的对象参数集合作为数据载体，结合上一步骤的行，这一步骤的列我们能够构建出具有表头信息和每行数据
  的完成的表格 （dataFrame）
*/
    val schema = types.StructType(
      List(
          StructField("id", DataTypes.IntegerType, false),
          StructField("name", DataTypes.StringType, false),
          StructField("gender", DataTypes.IntegerType, false),
          StructField("height", DataTypes.DoubleType, false)
      )
    )
    /*createDataFrame其中的一个构造器中允许传递RDD和StrutType对象创建DataFrame，也就是需要完整的行列信息才能创建表*/
    val dataFrame = spark.createDataFrame(row,schema).show()
    spark.stop()
  }
}
