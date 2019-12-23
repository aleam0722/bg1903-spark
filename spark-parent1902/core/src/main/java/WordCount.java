import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main (String[] args){
        /*step.1 创建编程入口*/
        /*观察JvaSparkContext源码发现，创建JavaSparkContext对象需要传传入SparkConf对象，因此需要创建SparkConf*/
        SparkConf sc = new SparkConf();
        /*在SparkConf对象中设置连接的master URL字符串*/
        sc.setMaster("local[*]");
        sc.setAppName(WordCount.class.getSimpleName());
        /*创建spark程序入口JavaSparkContext的对象*/
        JavaSparkContext jsc = new JavaSparkContext(sc);


        /*step2.加载数据*/
        final JavaRDD<String> stringJavaRDD = jsc.textFile("E:/word.txt");

        /*step.3对加载的数据进行操作*/
        JavaRDD<String> words = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });

        JavaPairRDD<String, Integer> kvPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> result = kvPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("stringIntegerTuple2 = " + stringIntegerTuple2);
            }
        });
    }
}
