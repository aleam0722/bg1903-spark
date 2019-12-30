import org.apache.spark.sql.SparkSession

object SparkSqlOp {
  /*sparkSession的构建*/
  val spark = SparkSession.builder().master("local[*]").appName("SparkSqlOp").getOrCreate()
  def main(args: Array[String]): Unit = {
//    loadData(spark)
//    showData(spark)
//    simpleSelect(spark)
    clumnOperation(spark)
  }
  def loadData(spark:SparkSession):Unit = {
    /*加载一个json格式的数据*/
    val dataFrame = spark.read.json("File:///E:/sparkData/sql/account.json")
//    dataFrame.printSchema()

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
    spark.stop()
  }

  def showData(spark: SparkSession): Unit = {
    val dataFrame = spark.read.json("File:///E:/sparkData/sql/account.json")
    dataFrame.show()
    spark.stop()

    /**
     * +--------------+--------------------+---+-------+-----------+--------------------+---------+---------+------+---------+-----+
     * |account_number|             address|age|balance|       city|               email| employer|firstname|gender| lastname|state|
     * +--------------+--------------------+---+-------+-----------+--------------------+---------+---------+------+---------+-----+
     * |             1|     880 Holmes Lane| 32|  39225|     Brogan|amberduke@pyrami.com|   Pyrami|    Amber|     M|     Duke|   MD|
     * |             6|  671 Bristol Street| 36|   5686|      Dante|hattiebond@netagy...|   Netagy|   Hattie|     M|     Bond|   MD|
     * |            13|  789 Madison Street| 28|  32838|      Nogal|nanettebates@quil...|  Quility|  Nanette|     F|    Bates|   VA|
     * |            18|467 Hutchinson Court| 33|   4180|      Orick| daleadams@boink.com|    Boink|     Dale|     M|    Adams|   MD|
     * |            20|     282 Kings Place| 36|  16418|     Ribera|elinorratliff@sce...| Scentric|   Elinor|     M|  Ratliff|   MD|
     * |            25|   171 Putnam Avenue| 39|  40540|  Nicholson|virginiaayala@fil...| Filodyne| Virginia|     F|    Ayala|   VA|
     * |            32|  702 Quentin Street| 34|  48086|    Veguita|dillardmcpherson@...| Quailcom|  Dillard|     F|Mcpherson|   VA|
     * |            37|  826 Fillmore Place| 39|  18612| Tooleville|mcgeemooney@rever...| Reversus|    Mcgee|     M|   Mooney|   VA|
     * |            44|502 Baycliff Terrace| 37|  34487|  Yardville|aureliaharding@or...|  Orbalix|  Aurelia|     M|  Harding|   VA|
     * |            49| 451 Humboldt Street| 23|  29104|   Sunriver|fultonholt@anocha...|   Anocha|   Fulton|     F|     Holt|   RI|
     * |            51|    334 River Street| 31|  14097|Jacksonburg|burtonmeyers@beza...|    Bezal|   Burton|     F|   Meyers|   RI|
     * |            56|     857 Tabor Court| 32|  14992|  Sunnyside|josienelson@emtra...|   Emtrac|    Josie|     M|   Nelson|   RI|
     * |            63| 510 Sedgwick Street| 30|   6077|   Guilford|hughesowens@valpr...| Valpreal|   Hughes|     F|    Owens|   KS|
     * |            68|     927 Bay Parkway| 25|  44214|    Shawmut| hallkey@eventex.com|  Eventex|     Hall|     F|      Key|   KS|
     * |            70|     685 School Lane| 33|  38172|   Chestnut|deidrethompson@ne...| Netplode|   Deidre|     F| Thompson|   RI|
     * |            75| 166 Irvington Place| 22|  40500|  Limestone|sandovalkramer@ov...| Overfork| Sandoval|     F|   Kramer|   RI|
     * |            82|   195 Bayview Place| 39|  41412|Summerfield|concettabarnes@fi...|  Fitcore| Concetta|     F|   Barnes|   KS|
     * |            87|  446 Halleck Street| 22|   1133|   Coalmont|hewittkidd@isolog...|Isologics|   Hewitt|     M|     Kidd|   KS|
     * |            94|  183 Kathleen Court| 30|  41060| Cornucopia|brittanycabrera@m...|   Mixers| Brittany|     F|  Cabrera|   KS|
     * |            99|  806 Rockwell Place| 39|  47159|      Shaft|ratliffheath@zapp...|   Zappix|  Ratliff|     F|    Heath|   KS|
     * +--------------+--------------------+---+-------+-----------+--------------------+---------+---------+------+---------+-----+
     */

  }

  def simpleSelect(spark: SparkSession): Unit = {
    val dataFrame = spark.read.json("File:///e:/sparkData/sql/account.json")
    dataFrame.select("account_number", "address").show()
    spark.stop()

    /**
     * result----->
     * +--------------+--------------------+
     * |account_number|             address|
     * +--------------+--------------------+
     * |             1|     880 Holmes Lane|
     * |             6|  671 Bristol Street|
     * |            13|  789 Madison Street|
     * |            18|467 Hutchinson Court|
     * |            20|     282 Kings Place|
     * |            25|   171 Putnam Avenue|
     * |            32|  702 Quentin Street|
     * |            37|  826 Fillmore Place|
     * |            44|502 Baycliff Terrace|
     * |            49| 451 Humboldt Street|
     * |            51|    334 River Street|
     * |            56|     857 Tabor Court|
     * |            63| 510 Sedgwick Street|
     * |            68|     927 Bay Parkway|
     * |            70|     685 School Lane|
     * |            75| 166 Irvington Place|
     * |            82|   195 Bayview Place|
     * |            87|  446 Halleck Street|
     * |            94|  183 Kathleen Court|
     * |            99|  806 Rockwell Place|
     * +--------------+--------------------+
     */
  }

  def clumnOperation(spark: SparkSession): Unit = {
    val dataFrame = spark.read.json("File:///e:/sparkData/sql/account.json")
    import spark.implicits._
    dataFrame.select($"account_number"-1).show()
    spark.stop()
    /**
     * +--------------------+
     * |(account_number - 1)|
     * +--------------------+
     * |                   0|
     * |                   5|
     * |                  12|
     * |                  17|
     * |                  19|
     * |                  24|
     * |                  31|
     * |                  36|
     * |                  43|
     * |                  48|
     * |                  50|
     * |                  55|
     * |                  62|
     * |                  67|
     * |                  69|
     * |                  74|
     * |                  81|
     * |                  86|
     * |                  93|
     * |                  98|
     * +--------------------+
     * */

  }


}
