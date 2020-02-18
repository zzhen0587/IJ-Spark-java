import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
/*
* 自定义 key 排序方式
*
* */
// author zzhen
// 2020/2/18
public class SecondarySort {
    public static void main(String[] args) {
        //initial conf of sparkContext
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");// 集群运行，删除setMaster
        //
        JavaSparkContext sc = new JavaSparkContext(conf);
        //
        List<Tuple2<Integer, Integer>> arr = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 3),
                new Tuple2<Integer, Integer>(5, 10),
                new Tuple2<Integer, Integer>(7, 3),
                new Tuple2<Integer, Integer>(3, 5),
                new Tuple2<Integer, Integer>(3, 3),
                new Tuple2<Integer, Integer>(6, 9),
                new Tuple2<Integer, Integer>(9, 9),
                new Tuple2<Integer, Integer>(4, 6),
                new Tuple2<Integer, Integer>(7, 6),
                new Tuple2<Integer, Integer>(8, 9)
        );
        JavaRDD<Tuple2<Integer, Integer>> paralleliz = sc.parallelize(arr);

        JavaPairRDD<SecondarySortKey, Tuple2<Integer, Integer>> secondarySortKeyTuple2JavaPairRDD = paralleliz.mapToPair(
                new PairFunction<Tuple2<Integer, Integer>, SecondarySortKey, Tuple2<Integer, Integer>>() {// input,output
                    @Override
                    public Tuple2<SecondarySortKey, Tuple2<Integer, Integer>> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                        SecondarySortKey key = new SecondarySortKey(
                                Integer.valueOf(integerIntegerTuple2._1),
                                Integer.valueOf(integerIntegerTuple2._2)
                        );
                        return new Tuple2<SecondarySortKey, Tuple2<Integer, Integer>>(key, integerIntegerTuple2);
                    }
                });
        JavaPairRDD<SecondarySortKey, Tuple2<Integer, Integer>> secondarySortKeyTuple2JavaPairRDD1 = secondarySortKeyTuple2JavaPairRDD.sortByKey(false);
        secondarySortKeyTuple2JavaPairRDD1.foreach(new VoidFunction<Tuple2<SecondarySortKey, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<SecondarySortKey, Tuple2<Integer, Integer>> secondarySortKeyTuple2Tuple2) throws Exception {
                System.out.println(secondarySortKeyTuple2Tuple2._2._1+"   "+secondarySortKeyTuple2Tuple2._2._2);
            }
        });
        sc.close();
    }
}
