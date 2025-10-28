package com.example.dataFramesSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class exercice1 {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("exercice1").master("local[*]").getOrCreate();

        Dataset<Row> df =sparkSession.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("incidents.csv");

        //Afficher le nombre d’incidents par service.
        Dataset<Row> incidentsParService =df.groupBy("service")
                                            .agg(count("id")
                                                    .as("nombreIncidents"));
        incidentsParService.show();

        //Afficher les deux années où il a y avait plus d’incidents.
        df.groupBy(year(col("date")))
                .agg(count("id").as("nombreIncidents"))
                .orderBy(col("nombreIncidents").desc())
                .show(2);
    }
}
