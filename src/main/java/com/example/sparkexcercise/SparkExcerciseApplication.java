package com.example.sparkexcercise;

import com.example.sparkexcercise.module1.Join;
import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class SparkExcerciseApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkExcerciseApplication.class, args);

//        reduce();
//        map();
//        tuple();
//        pairRdd();
//        refactorPairRdd();
//        flatMapsAndFilters();
//        diskFile();
//        takeAndReplaceAll();
//        sortAndCoalesce();
//        Join.leftOuterJoin();
        Join.cartesian();
    }


    private static void reduce() {
        List<Double> inputData = new ArrayList<Double>();
        inputData.add(35.5);
        inputData.add(12.499943);
        inputData.add(90.32);
        inputData.add(20.32);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        Double result = myRdd.reduce((v1, v2) -> v1 + v2);

        System.out.println("result = " + result);
//        myRdd.reduce(Double::sum);

        sc.close();
    }

    private static void map() {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((v1, v2) -> v1 + v2);
        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));

//        sqrtRdd.foreach(value -> System.out.println("value = " + value));
//        sqrtRdd.foreach(System.out::println);
        sqrtRdd.collect().forEach(System.out::println);
        //how many elements in sqrtRdd
        long count1 = sqrtRdd.count();

        //using just map and reduce
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce(Long::sum);

        sc.close();
    }

    private static void tuple() {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

        JavaRDD<IntegerWithSquare> sqrtRdd = originalIntegers.map(IntegerWithSquare::new);
        Tuple2<Integer, Double> myValue = new Tuple2<>(9, 3.0);
        JavaRDD<Tuple2> myTupleRdd = originalIntegers.map(value -> new Tuple2<Integer, Double>(value, Math.sqrt(value)));
        sc.close();
    }

    private static void pairRdd() {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

        JavaPairRDD<String, String> pairRDD = originalLogMessages.mapToPair(rawValue -> {
            String[] columns = rawValue.split(":");
            String level = columns[0];
            String date = columns[1];
            return new Tuple2<>(level, date);
        });

        //groupByKey 대체 -> count 용도로 쓸때
        JavaPairRDD<String, Long> pairRDD2 = originalLogMessages.mapToPair(rawValue -> {
            String[] columns = rawValue.split(":");
            String level = columns[0];
            return new Tuple2<>(level, 1L);
        });
        JavaPairRDD<String, Long> groupByCount = pairRDD2.reduceByKey((v1, v2) -> v1 + v2);


//        pairRDD.aggregateByKey();
//        pairRDD.groupByKey();
        sc.close();
    }

    private static void refactorPairRdd() {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey(Long::sum)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        sc.close();
    }

    private static void groupBy() {
        //실무 사용 금지
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

        sc.close();
    }

    private static void flatMapsAndFilters() {
        //실무 사용 금지
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputData)
                .flatMap(v -> Arrays.asList(v.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .foreach(w -> System.out.println("w = " + w));

        sc.close();
    }

    private static void diskFile() {
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input-spring.txt"); //hdfs

        initialRdd
                .flatMap(v -> Arrays.asList(v.split(" ")).iterator())
                .foreach(w -> System.out.println("w = " + w));

    }

    private static void takeAndReplaceAll() {
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input-spring.txt"); //hdfs

        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);
        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(Util::isNotBoring);
        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey(Long::sum);
        totals.foreach(v-> System.out.println("v = " + v));

    }
    private static void sortAndCoalesce() {
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input-spring.txt"); //hdfs

        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);
        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(Util::isNotBoring);
        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey(Long::sum);
        //value 가 높은순으로 나열해야하는데 sortByValue는 존재하지않아..
        //key, value switch 하는 트릭사용
        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false); // false -> descending
//        List<Tuple2<Long, String>> take = sorted.take(50);
        List<Tuple2<Long, String>> take = sorted.collect();
        take.forEach(v-> System.out.println("v = " + v));

    }
    private static void aws() {
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input-spring.txt"); //hdfs
//        JavaRDD<String> initialRdd = sc.textFile("s3n://{yourStorageDomain/input.txt"); //hdfs

        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);
        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(Util::isNotBoring);
        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey(Long::sum);
        //value 가 높은순으로 나열해야하는데 sortByValue는 존재하지않아..
        //key, value switch 하는 트릭사용
        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false); // false -> descending
//        List<Tuple2<Long, String>> take = sorted.take(50);
        List<Tuple2<Long, String>> take = sorted.collect();
        take.forEach(v-> System.out.println("v = " + v));

    }

}