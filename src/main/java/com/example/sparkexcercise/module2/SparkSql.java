package com.example.sparkexcercise.module2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

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
}