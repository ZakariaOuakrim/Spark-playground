package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Exercice1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Exercice1").setMaster( "local[*]" );
        JavaSparkContext sparkContext = new JavaSparkContext( conf );

        JavaRDD<String> lineInFile = sparkContext.textFile("ventes.txt");
        //fera9
        JavaPairRDD<String,Double> cityAndSale = lineInFile.mapToPair(line->{
            String[] words = line.split(" ");
            String city = words[1];
            Double sale=Double.parseDouble(words[3]);
            return new Tuple2<>(city,sale);
        });
        //jma3
        JavaPairRDD<String,Double> cityToSales = cityAndSale.reduceByKey((a,b)->b+b);
        cityToSales.foreach(value-> {
            System.out.println("city "+value._1+" Sales "+ value._2);
        });


    }
}
