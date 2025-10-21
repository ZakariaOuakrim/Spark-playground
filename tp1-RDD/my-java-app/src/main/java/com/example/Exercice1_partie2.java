package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercice1_partie2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Exercice1_partie2").setMaster( "local[*]" );
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines=sparkContext.textFile("ventes.txt");

        // line ((ville,année),sale)
        JavaPairRDD<Tuple2<String, String>, Double> cityYearSale = lines.mapToPair(line -> {
            String[] words = line.split(" ");
            String city = words[1];
            String date = words[0];
            String year = date.split("-")[0];
            Double price = Double.parseDouble(words[3]);
            Tuple2<String, String> cityYear = new Tuple2<>(city, year);
            return new Tuple2<>(cityYear, price);
        });

        //jm3
        JavaPairRDD<Tuple2<String,String>,Double> result=cityYearSale.reduceByKey((a,b)->a+b);

        result.collect().forEach(tuple -> {
            Tuple2<String, String> cle = tuple._1();
            double total = tuple._2();
            System.out.println("Ville: " + cle._1() + ", Année: " + cle._2() + " → Total: " + total);
        });
    }
}
