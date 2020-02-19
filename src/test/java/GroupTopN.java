import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/*
 * author:zzhen
 * 2020/2/19
 * 分组TopN
 * */
public class GroupTopN {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("GroupTopN")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(new Tuple2<String, Integer>("class1", 2),
                new Tuple2<String, Integer>("class1", 2),
                new Tuple2<String, Integer>("class1", 3),
                new Tuple2<String, Integer>("class2", 6),
                new Tuple2<String, Integer>("class2", 4),
                new Tuple2<String, Integer>("class2", 7),
                new Tuple2<String, Integer>("class1", 13),
                new Tuple2<String, Integer>("class1", 12),
                new Tuple2<String, Integer>("class2", 8),
                new Tuple2<String, Integer>("class1", 9),
                new Tuple2<String, Integer>("class2", 6)
        );

//        JavaRDD<Tuple2<String, Integer>> parallelize = sc.parallelize(tuple2s);
//        parallelize.groupByKey();
        /*
         * JavaRDD<Tuple2<String, Integer>> 不能直接使用 groupByKey()，要转成 JavaPairRDD<String, Integer>
         * */
        JavaRDD<Tuple2<String, Integer>> parallelize = sc.parallelize(tuple2s);
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = parallelize.mapToPair(
                new PairFunction<Tuple2<String, Integer>, String, Integer>() {

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._1, tuple2._2);
                    }
                }
        );
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        JavaRDD<Tuple2<String, Iterable<Integer>>> map = stringIterableJavaPairRDD.map(
                new Function<Tuple2<String, Iterable<Integer>>, Tuple2<String, Iterable<Integer>>>() {
                    @Override
                    public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                        Integer[] top = new Integer[3];
                        Iterator<Integer> iterator = stringIterableTuple2._2.iterator();
                        while (iterator.hasNext()) {
                            Integer score = iterator.next();
                            for (int i = 0; i < top.length; i++) {
                                if (top[i] == null) {
                                    top[i] = score;
                                    break;
                                } else if (score > top[i]) {
                                    for(int j = top.length-1;j>i;j--){
                                        top[j] = top[j-1];
                                    }
                                    top[i] = score;
                                    break;
                                }
                            }
                        }
                        return new Tuple2<String, Iterable<Integer>>(stringIterableTuple2._1, Arrays.asList(top));
                    }
                });
        map.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1);
                Iterator<Integer> iterator = stringIterableTuple2._2.iterator();
                while(iterator.hasNext()){
                    Integer next = iterator.next();
                    System.out.println(next);
                }
            }
        });

        sc.close();
}
}
