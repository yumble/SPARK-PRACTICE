package com.example.sparkexcercise.module1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class WalkThrough {
    static JavaPairRDD<Integer, Integer> viewData;
    static JavaPairRDD<Integer, Integer> chapterData;
    static JavaPairRDD<Integer, String> titlesData;

    public static void problem1() {
        JavaSparkContext sc = start();

        JavaPairRDD<Integer, Integer> switchKeyAndValue = chapterData.mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey(Integer::sum);
        switchKeyAndValue.foreach(v -> System.out.println("v = " + v));

        sc.close();

    }

    public static void problem2() {
        JavaSparkContext sc = start();

        JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey(Integer::sum);
        chapterCountRdd.foreach(v -> System.out.println("v = " + v));

        //Step 1 - remove and duplicated views
        //(UserId, ChapterId)
        viewData = viewData.distinct();
        viewData.foreach(v -> System.out.println("v = " + v));

        //Step 2 - RDD Form - (chapterId, (userId, courseId))
        viewData = viewData.mapToPair(row -> new Tuple2<>(row._2, row._1));
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = viewData.join(chapterData);
        joinedRdd.foreach(v -> System.out.println("v = " + v));

        //Step 3 - don't need chapterIds, setting up for a reduce
        //((userId,courseId), 1)
        JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRdd.mapToPair(row -> new Tuple2<>(row._2, 1L));
        step3.foreach(v -> System.out.println("v = " + v));

        //Step 4 - count how many views for each user per course
        //((userId, courseId), sum)
        step3 = step3.reduceByKey(Long::sum);
        step3.foreach(v -> System.out.println("v = " + v));

        //Step 5 - Drop the User Id
        //(courseId, sum)
        JavaPairRDD<Integer, Long> step5 = step3.mapToPair(row -> new Tuple2<>(row._1._2, row._2));
        step5.foreach(v -> System.out.println("v = " + v));

        //Step 6 - Check How Many Chapters, add in the total chapter count
        //(courseId, (views, chapterCount))
        JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chapterCountRdd);
        step6.foreach(v -> System.out.println("v = " + v));

        //Step 7 - Percentage of Course
        JavaPairRDD<Integer, Double> step7 = step6.mapValues(v -> (double) v._1 / v._2);
        step7.foreach(v -> System.out.println("v = " + v));

        //Step 8 - convert to scores
        JavaPairRDD<Integer, Long> step8 = step7.mapValues(v -> {
            if (v > 0.9) return 10L;
            if (v > 0.5) return 4L;
            if (v > 0.2) return 2L;
            return 0L;
        });
        step8.foreach(v -> System.out.println("v = " + v));

        //Step 9 - Add up the Total scores
        JavaPairRDD<Integer, Long> step9 = step8.reduceByKey(Long::sum);
        step9.foreach(v -> System.out.println("v = " + v));

        //Step 10 - join Title
        JavaPairRDD<Integer, Tuple2<Long, String>> step10 = step9.join(titlesData);
        step10.foreach(v -> System.out.println("v = " + v));
        JavaPairRDD<Long, String> step11 = step10.mapToPair(row -> new Tuple2<>(row._2._1, row._2._2));
        step11.sortByKey(false).collect().forEach(v-> System.out.println("v = " + v));
        
        sc.close();

    }


    //초기 세팅
    public static JavaSparkContext start() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = true;

        viewData = setUpViewDataRdd(sc, testMode);
        chapterData = setUpChapterDataRdd(sc, testMode);
        titlesData = setUpTitlesDataRdd(sc, testMode);

        // TODO - over to you!
//        sc.close();
        return sc;
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
