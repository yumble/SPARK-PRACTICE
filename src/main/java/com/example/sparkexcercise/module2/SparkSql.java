package com.example.sparkexcercise.module2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSql {

    public static void sparkSql() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");
        dataset.show();


        long numberOfRows = dataset.count();
        System.out.println("numberOfRows = " + numberOfRows);

        Row firstRow = dataset.first();

        String subject = firstRow.get(2).toString(); //index number
        System.out.println("subject = " + subject);
        String subject2 = firstRow.getAs("subject").toString();//index number
        System.out.println("subject2 = " + subject2);

//        String[] columns = dataset.columns();

        //ver1
        Dataset<Row> modernArtResultsUsingExpressions = dataset.filter(" subject = 'Modern Art' AND year >= 2007 ");
        //ver2
        Dataset<Row> modernArtResultsUsingLambdas = dataset
                .filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art")
                        && Integer.parseInt(row.getAs("year")) >= 2007);
        //ver3
        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");
        Dataset<Row> modernArtResultsUsingColumns = dataset
                .filter(subjectColumn.equalTo("Modern Art")
                        .and(yearColumn.geq(2007)));
        //ver 3-1
        Dataset<Row> modernArtResultsUsingColumns2 = dataset
                .filter(col("subject").equalTo("Modern Art")
                .and(col("year").geq(2007)));


        modernArtResultsUsingColumns.show();

        spark.close();
    }

    public static void fullSql(){
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        dataset.createOrReplaceTempView("my_students_table");

        Dataset<Row> results = spark.sql("select distinct(year) from my_students_table where subject = 'French' order by year desc");

        results.show();

        spark.close();

    }
    public static void inMemory() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        List<Row> inMemoryList = new ArrayList<>();
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 04:09:32"));
        inMemoryList.add(RowFactory.create("FATAL", "2016-12-31 03:08:32"));
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 03:09:32"));
        inMemoryList.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemoryList.add(RowFactory.create("FATAL", "2015-4-21 19:23:02"));


        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemoryList, schema);

        dataset.show();

        spark.close();
    }
    public static void groupingAndAggregation() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        List<Row> inMemoryList = new ArrayList<>();
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 04:09:32"));
        inMemoryList.add(RowFactory.create("FATAL", "2016-12-31 03:08:32"));
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 03:09:32"));
        inMemoryList.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemoryList.add(RowFactory.create("FATAL", "2015-4-21 19:23:02"));


        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemoryList, schema);

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql("select level, collect_list(datetime) from logging_table group by level order by level");

        results.show();

        spark.close();
    }
    public static void dateFormatting(){
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        List<Row> inMemoryList = new ArrayList<>();
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 04:09:32"));
        inMemoryList.add(RowFactory.create("FATAL", "2016-12-31 03:08:32"));
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 03:09:32"));
        inMemoryList.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemoryList.add(RowFactory.create("FATAL", "2015-4-21 19:23:02"));


        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemoryList, schema);

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");

        results.show();

        spark.close();
    }
    public static void multipleGrouping(){
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        List<Row> inMemoryList = new ArrayList<>();
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 04:09:32"));
        inMemoryList.add(RowFactory.create("FATAL", "2016-12-31 03:08:32"));
        inMemoryList.add(RowFactory.create("WARN", "2016-12-31 03:09:32"));
        inMemoryList.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemoryList.add(RowFactory.create("FATAL", "2015-4-21 19:23:02"));


        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemoryList, schema);

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month");

        results.show();

        spark.close();
    }
    public static void multipleGroupingByBigLogTxt(){
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month");

//        results.show(false); // column 자르지 말라는 말인듯. row수는 그대로 자른다. -> row수 늘리려면파라미터로 숫자를 줘야함.

        results.show(100);

        results.createOrReplaceTempView("results_table");

        Dataset<Row> totals = spark.sql("select sum(total) from results_table");

        totals.show();

        spark.close();
    }
}