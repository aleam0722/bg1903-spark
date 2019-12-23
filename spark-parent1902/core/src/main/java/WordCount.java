import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
    public static void main (String[] args){

        /*观察JvaSparkContext源码发现，创建JavaSparkContext对象需要传传入SparkConf对象，因此需要创建SparkConf*/
        SparkConf sc = new SparkConf();
        /*在SparkConf对象中设置连接的master URL字符串*/
        sc.setMaster("local[*]");
        sc.setAppName(WordCount.class.getSimpleName());
        /*创建spark程序入口JavaSparkContext的对象*/
        JavaSparkContext jsc = new JavaSparkContext(sc);


        /*step2.加载数据*/
        JavaRDD<String> stringJavaRDD = jsc.textFile("E:/word.txt");
    }
}
