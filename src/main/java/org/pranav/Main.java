package org.pranav;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = Arrays.asList(23123, 23, 444, 7676);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        try(JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<Integer> javaRDD = sc.parallelize(inputData);

            Integer reduce = javaRDD.reduce(Integer::sum);
            System.out.println("reduce = " + reduce);

            JavaRDD<Double> map = javaRDD.map(Math::sqrt);
            map.collect().forEach(System.out::println);
            System.out.println("map = " + map);

            System.out.println(map.count());
            Integer reduce1 = javaRDD.map(x -> 1).reduce(Integer::sum);
            System.out.println("reduce1 = " + reduce1);

            Tuple2<Integer, Double> tuple2 = new Tuple2<>(9, 3.0);
            System.out.println("tuple2 = " + tuple2);

            JavaRDD<Tuple2<Integer, Double>> tuple2JavaRDD = javaRDD.map(d -> new Tuple2<>(d, Math.sqrt(d)));
            System.out.println("tuple2JavaRDD = " + tuple2JavaRDD);
            tuple2JavaRDD.collect().forEach(x -> System.out.println("x = " + x));
        }
    }
}
