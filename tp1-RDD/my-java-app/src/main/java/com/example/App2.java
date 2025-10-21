package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class App2 {
    public static void main(String[] args) {
        SparkConf sparkConf= new SparkConf().setAppName("Word Count").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rddLines= sparkContext.textFile("file.txt");

        JavaRDD<String> rddWords = rddLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String,Integer> rddPairWords= rddWords.mapToPair(word->new Tuple2<>(word,1));

        JavaPairRDD<String,Integer> rddWordCount=rddPairWords.reduceByKey((a, b) -> a+b);

        rddWordCount.foreach(word->System.out.println(word._1+" "+word._2));

    }
}
