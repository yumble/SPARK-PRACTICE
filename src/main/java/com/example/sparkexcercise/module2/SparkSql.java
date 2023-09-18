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

import javax.xml.crypto.Data;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.*;

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

    public static void fullSql() {
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


        StructField[] fields = new StructField[]{
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


        StructField[] fields = new StructField[]{
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

    public static void dateFormatting() {
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


        StructField[] fields = new StructField[]{
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

    public static void multipleGrouping() {
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


        StructField[] fields = new StructField[]{
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

    public static void multipleGroupingByBigLogTxt() {
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

    public static void ordering() {
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

        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
                " count(1) as total from logging_table " +
                " group by level, month " +
                " order by cast(first(date_format(datetime, 'M')) as int), level ");

        results.show(100);

        spark.close();
    }

    public static void dataFrameVsDateSet() {
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

//        dataset = dataset.selectExpr("level", "date_format(datetime, 'MMMM') as month");
        dataset = dataset.select(col("level"),
                functions.date_format(col("datetime"), "MMMM").alias("month"));

        dataset.show(100);

        spark.close();
    }

    public static void groupByInJava() {
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

        dataset = dataset.select(col("level"),
                functions.date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

        dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();

        dataset = dataset.orderBy(col("monthnum"), col("level"));

        dataset = dataset.drop(col("monthnum"));

        dataset.show(100);

        spark.close();
    }

    public static void pivotTable() {
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

        dataset = dataset.select(col("level"),
                functions.date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

        Object[] months = new Object[]{"NoMonth", "January", "February", "March", "April",
                "May", "June", "July", "August", "September", "October", "November", "December"};
        List<Object> columns = Arrays.asList(months);

        dataset = dataset.groupBy("level").pivot("month", columns).count()
                .na().fill(0);

        dataset.show(100);

        spark.close();
    }

    public static void moreAggregations() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
//                .option("inferSchema", true) -> 사용 금지
                .csv("src/main/resources/exams/students.csv");

        dataset = dataset.groupBy("subject")
                .agg(max(col("score")).alias("max score"),
                        min(col("score").cast(DataTypes.IntegerType)).alias("min score"));

        dataset.show(100);


    }

    public static void practicalSession() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
//                .option("inferSchema", true) -> 사용 금지
                .csv("src/main/resources/exams/students.csv");

        dataset = dataset.groupBy("subject")
                .pivot("year")
                .agg(round(avg(col("score")), 2).alias("average"),
                        round(stddev(col("score")), 2).alias("stddev"));

        dataset.show();

        spark.close();
    }

    public static void userDefined() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///Users/h._.jxxn/tmp/")
                .getOrCreate();

        spark.udf().register("hasPassed", (String grade) -> grade.equals("A+"),
                DataTypes.BooleanType);

        spark.udf().register("hasPassed1", (String grade) -> {
                    return grade.startsWith("A") || grade.startsWith("B");
                },
                DataTypes.BooleanType);

        spark.udf().register("hasPassed2", (String grade, String subject) -> {
                    if (subject.equals("Biology")) {
                        return grade.startsWith("A");
                    }
                    return grade.startsWith("A") || grade.startsWith("B");
                },
                DataTypes.BooleanType);

        Dataset<Row> dataset = spark.read()
                .option("header", true)
//                .option("inferSchema", true) -> 사용 금지
                .csv("src/main/resources/exams/students.csv");

//        dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade")));
        dataset = dataset.withColumn("pass2", callUDF("hasPassed2", col("grade"), col("subject")));

        dataset.show();

        spark.close();
    }

    public static void userDefined2() {
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

        SimpleDateFormat input = new SimpleDateFormat("MMMM", Locale.ENGLISH);
        SimpleDateFormat output = new SimpleDateFormat("M");

        spark.udf().register("monthNum", (String month) -> {
                    Date inputDate = input.parse(month);
                    return Integer.parseInt(output.format(inputDate));
                },
                DataTypes.IntegerType);

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql(
                "select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logging_table group by level, month order by monthNum(month), level"
        );

        results.show(100);

        spark.close();
    }

    public static void performance() {
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

//        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
//                " count(1) as total from logging_table " +
//                " group by level, month " +
//                " order by cast(first(date_format(datetime, 'M')) as int), level ");

        dataset = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

        dataset = dataset.groupBy("level", "month", "monthnum")
                .count().as("total")
                .orderBy("monthnum");
        dataset = dataset.drop("monthnum");

        dataset.show(100);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }

}