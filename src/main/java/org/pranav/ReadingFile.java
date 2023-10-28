package org.pranav;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReadingFile {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C://Hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> inputData = sc.textFile("src/main/resources/input.txt");

            inputData
                    .flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                    .map(s -> s.replaceAll("[^A-Za-z]", ""))
                    .filter(s -> !s.trim().isEmpty())
                    .mapToPair(data -> new Tuple2<>(data, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair(x-> new Tuple2<>(x._2(), x._1()))
                    .sortByKey(false)
                    .take(50)
                    .forEach(System.out::println);

        }
    }
}
