package com.example.sparkexcercise.module1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Join {
    static JavaPairRDD<Integer, Integer> visits ;
    static JavaPairRDD<Integer, String> users;
    public static JavaSparkContext common() {
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        visits = sc.parallelizePairs(visitsRaw);
        users = sc.parallelizePairs(usersRaw);

        return sc;
    }
    public static void innerJoin(){
        JavaSparkContext sc = common();

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
        joinedRdd.foreach(v -> System.out.println("v = " + v));

        sc.close();
    }
    public static void leftOuterJoin(){
        JavaSparkContext sc = common();

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
        joinedRdd.foreach(v -> System.out.println(v._2._2.orElse("blank").toUpperCase()));

        //rightJoin
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinRdd = visits.rightOuterJoin(users);
        rightOuterJoinRdd.foreach(v -> System.out.println(v._2._1.orElse(0)));

        sc.close();
    }

    public static void cartesian(){
        JavaSparkContext sc = common();

        //cartesian
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesian = visits.cartesian(users);
        cartesian.foreach(v -> System.out.println(v));

        sc.close();
    }
}
