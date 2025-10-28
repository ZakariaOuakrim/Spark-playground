package com.example.dataFramesSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class app1 {
    public static void main(String[] args) {
        SparkSession sparkSession= SparkSession.builder()
                .appName("TP Spark SQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df1 =sparkSession.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("students.csv");

        df1.printSchema();
        df1.show();

        //best score
        df1.orderBy(col("note").desc()).show(1);

        //avg score
        df1.select(avg("note").as("note moyenne")).show();

        //nbr student who got the
        df1.where("note>=10").show();

    }
}
