import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
    public static void main (String[] args){
        /*观察JavaSparkContext源码发现，创建JavaSparkContext对象需要传传入SparkConf对象，因此需要创建SparkConf*/
        SparkConf sc = new SparkConf();
        /*创建spark程序入口JavaSparkContext的对象*/
        JavaSparkContext jsc = new JavaSparkContext();
    }
}
