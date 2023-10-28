package org.pranav;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BigData {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C://Hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");


        List<Tuple2<Integer, Integer>> userCourseId = new ArrayList<>();
        userCourseId.add(new Tuple2<>(14, 96));
        userCourseId.add(new Tuple2<>(14, 97));
        userCourseId.add(new Tuple2<>(13, 96));
        userCourseId.add(new Tuple2<>(13, 96));
        userCourseId.add(new Tuple2<>(13, 96));
        userCourseId.add(new Tuple2<>(14, 99));
        userCourseId.add(new Tuple2<>(13, 100));

        List<Tuple2<Integer, Integer>> chapterCourseId = new ArrayList<>();
        chapterCourseId.add(new Tuple2<>(96, 1));
        chapterCourseId.add(new Tuple2<>(97, 1));
        chapterCourseId.add(new Tuple2<>(98, 1));
        chapterCourseId.add(new Tuple2<>(99, 2));
        chapterCourseId.add(new Tuple2<>(100, 3));
        chapterCourseId.add(new Tuple2<>(101, 3));
        chapterCourseId.add(new Tuple2<>(102, 3));
        chapterCourseId.add(new Tuple2<>(103, 3));
        chapterCourseId.add(new Tuple2<>(104, 3));
        chapterCourseId.add(new Tuple2<>(105, 3));
        chapterCourseId.add(new Tuple2<>(106, 3));
        chapterCourseId.add(new Tuple2<>(107, 3));
        chapterCourseId.add(new Tuple2<>(108, 3));
        chapterCourseId.add(new Tuple2<>(109, 3));

        List<Tuple2<Integer, String>> chapterIdTitles = new ArrayList<>();
        chapterIdTitles.add(new Tuple2<>(1, "John"));
        chapterIdTitles.add(new Tuple2<>(2, "Bob"));
        chapterIdTitles.add(new Tuple2<>(3, "Alan"));

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaPairRDD<Integer, Integer> userCourseIdRdd = sc.parallelizePairs(userCourseId).distinct();
            JavaPairRDD<Integer, Integer> chapterCourseIdRdd = sc.parallelizePairs(chapterCourseId);
            JavaPairRDD<Integer, String> chapterIdTitlesRdd = sc.parallelizePairs(chapterIdTitles);

            //get count of course per chapter
            JavaPairRDD<Integer, Long> courseIdCountRDD = chapterCourseIdRdd
                    .mapToPair(x -> new Tuple2<>(x._2(), 1L))
                    .reduceByKey(Long::sum);

            JavaPairRDD<Integer, Long> integerLongJavaPairRDD = userCourseIdRdd
                    .mapToPair(x -> new Tuple2<>(x._2(), 1L))
                    .reduceByKey(Long::sum);

            JavaPairRDD<Integer, Tuple2<Integer, Long>> join = chapterCourseIdRdd.join(integerLongJavaPairRDD);
            join.collect().forEach(System.out::println);

            JavaPairRDD<Integer, Long> integerLongJavaPairRDD1 = join.mapToPair(Tuple2::_2);
            JavaPairRDD<Integer, Tuple2<Long, Long>> join1 = integerLongJavaPairRDD1.join(courseIdCountRDD);
            join1.collect().forEach(System.out::println);

            JavaPairRDD<Integer, Integer> objectObjectJavaPairRDD = join1.mapToPair(x -> {
                Tuple2<Long, Long> longLongTuple2 = x._2();
                double l = (double) longLongTuple2._1() / longLongTuple2._2();
                int score = 0;
                if (l >= 0.9) {
                    score = 10;
                } else if (l >= 0.5) {
                    score = 4;
                } else if (l >= 0.25) {
                    score = 2;
                }
                return new Tuple2<>(x._1(), score);
            });

            objectObjectJavaPairRDD.collect().forEach(System.out::println);

            JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = objectObjectJavaPairRDD.reduceByKey(Integer::sum);
            integerIntegerJavaPairRDD.collect().forEach(System.out::println);

            integerIntegerJavaPairRDD.join(chapterIdTitlesRdd)
                    .mapToPair(Tuple2::_2)
                    .sortByKey(false)
                    .mapToPair(x -> new Tuple2<>(x._2(), x._1()))
                    .collect().forEach(System.out::println);

        }
    }

}
