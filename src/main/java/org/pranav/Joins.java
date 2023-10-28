package org.pranav;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Joins {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C://Hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        List<Tuple2<Integer, Integer>> visits = new ArrayList<>();
        visits.add(new Tuple2<>(4, 18));
        visits.add(new Tuple2<>(6, 4));
        visits.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> users = new ArrayList<>();
        users.add(new Tuple2<>(1, "John"));
        users.add(new Tuple2<>(2, "Bob"));
        users.add(new Tuple2<>(3, "Alan"));
        users.add(new Tuple2<>(4, "Doris"));
        users.add(new Tuple2<>(5, "Maybelle"));
        users.add(new Tuple2<>(6, "Rachel"));

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaPairRDD<Integer, Integer> visitRdd = sc.parallelizePairs(visits);
            JavaPairRDD<Integer, String> usersRdd = sc.parallelizePairs(users);

            //inner join
            JavaPairRDD<Integer, Tuple2<Integer, String>> innerJoin = visitRdd.join(usersRdd);
            innerJoin.collect().forEach(System.out::println);

            //left join
            JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoin = visitRdd.leftOuterJoin(usersRdd);
            leftOuterJoin.collect().forEach(System.out::println);

            //right join
            JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoin = visitRdd.rightOuterJoin(usersRdd);
            rightOuterJoin.collect().forEach(System.out::println);

            //full join
            JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoin = visitRdd.fullOuterJoin(usersRdd);
            fullOuterJoin.collect().forEach(System.out::println);

            //cross join
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> crossJoin = visitRdd.cartesian(usersRdd);
            crossJoin.collect().forEach(System.out::println);

        }
    }
}
