package com.example.dataFramesSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


public class app2 {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]").appName("Analyse de ventes ").getOrCreate();

        Dataset<Row> df1 = sparkSession.read().option("multiline",true).json("transactions.json");
        df1.createOrReplaceTempView("transactions");
        df1.printSchema();

        //afficher le total de vente par ville
        df1.groupBy(col("ville")).sum("montant").show();

        sparkSession.sql("select ville, sum(montant) from transactions group by ville").show();


    }
}
