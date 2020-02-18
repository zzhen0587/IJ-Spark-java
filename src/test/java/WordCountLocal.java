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

public class WordCountLocal {
    public static void main(String[] args) {
        //initial conf of sparkContext
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");// 集群运行，删除setMaster
        //
        JavaSparkContext sc = new JavaSparkContext(conf);
        //
        JavaRDD<String> lines = sc.textFile("C:\\Users\\86188\\IdeaProjects\\spark-study-java\\wordcountfile.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            // input :string
            // output:string
            public  Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        /*
         string -> (string,1)
         mapToPair
        */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });
        // according to key, count value
        final JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        // action operator to active
        stringIntegerJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + " " + stringIntegerTuple2._2);
            }
        });
        sc.close();
    }
}
