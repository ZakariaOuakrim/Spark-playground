package com.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class App 
{
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<Double> rdd1 = javaSparkContext.parallelize(Arrays.asList(11.0, 12.0, 3.0, 14.0, 15.0));
        JavaRDD<Double> rdd2=rdd1.map(x->x+1);
        JavaRDD<Double> rdd3=rdd2.filter(x->{
            System.out.println("OK");
            return x>10;
        });
        rdd3.persist(StorageLevel.MEMORY_ONLY());
        List<Double> result=rdd3.collect();
        result.forEach(System.out::println);

        List<Double> result1=rdd3.collect();
        result1.forEach(System.out::println);
        

        
    }
}
