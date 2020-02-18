import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class sortWordCount {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sortWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("");
        JavaRDD<String> stringJavaRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> objectObjectJavaPairRDD = stringJavaRDD.mapToPair(new PairFunction<String, String, Integer>(){
            @Override
            final public Tuple2<String,Integer> call(String t) throws Exception{
                return new Tuple2<String,Integer>(t,1);
            }
        });
        // string-integer
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = objectObjectJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        // integer-string
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = stringIntegerJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) {
                return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
            }
        });
        // sortbykey
        JavaPairRDD<Integer, String> integerStringJavaPairRDD1 = integerStringJavaPairRDD.sortByKey(false);// 降序排列
        // save or print
        integerStringJavaPairRDD1.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._1+" "+integerStringTuple2._2);
            }
        });
        sc.close();
    }
}
