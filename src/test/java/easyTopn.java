import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/*
* author:zzhen
* 2020/2/19
* 实现简单 TopN
* */
public class easyTopn {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("EasyTopN")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(new Tuple2<String, Integer>("a", 2),
                new Tuple2<String, Integer>("a", 2),
                new Tuple2<String, Integer>("b", 3),
                new Tuple2<String, Integer>("c", 6),
                new Tuple2<String, Integer>("y", 4),
                new Tuple2<String, Integer>("r", 7),
                new Tuple2<String, Integer>("i", 13),
                new Tuple2<String, Integer>("p", 12),
                new Tuple2<String, Integer>("q", 8),
                new Tuple2<String, Integer>("r", 9),
                new Tuple2<String, Integer>("u", 6)
        );
        // Integer-String
        JavaRDD<Tuple2<String, Integer>> parallelize = sc.parallelize(tuple2s);
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = parallelize.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<Integer, String>(tuple2._2,tuple2._1);
                    }
                }
        );
        // 降序
        JavaPairRDD<Integer, String> integerStringJavaPairRDD1 = integerStringJavaPairRDD.sortByKey(false);
        // 取前N
        List<Tuple2<Integer, String>> take = integerStringJavaPairRDD1.take(3);
        for(Tuple2<Integer,String> v:take){
            System.out.println(v._1+"  "+v._2);
        }
        sc.close();
    }
}
