package org.pranav;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairRdd {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> javaRDD = sc.parallelize(inputData);

            JavaPairRDD<String, String> pairRDD = javaRDD.mapToPair(data -> {
                String[] cols = data.split(":");
                return new Tuple2<>(cols[0], cols[1]);
            });

            //severe performance with groupByKey
            JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = pairRDD.groupByKey();
            stringIterableJavaPairRDD.collect().forEach(x -> System.out.println(x._1 + " has " + Iterables.size(x._2)));

            JavaPairRDD<String, Long> javaPairRDD = javaRDD.mapToPair(data -> {
                String[] cols = data.split(":");
                return new Tuple2<>(cols[0], 1L);
            });
            JavaPairRDD<String, Long> reduceByKey = javaPairRDD.reduceByKey(Long::sum);
            reduceByKey.collect().forEach(System.out::println);


            //compressing into 1 line
            sc.parallelize(inputData)
                    .mapToPair(data -> new Tuple2<>(data.split(":")[0], 1L))
                    .reduceByKey(Long::sum)
                    .collect().forEach(System.out::println);

        }
    }
}
