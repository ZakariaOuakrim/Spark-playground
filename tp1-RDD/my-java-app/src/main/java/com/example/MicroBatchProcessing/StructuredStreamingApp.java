package com.example.MicroBatchProcessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class StructuredStreamingApp {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession ss= SparkSession.builder()
                .appName("TP1")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType()
                .add("name", "string")
                .add("note", "double")
                .add("prénom","string")
                .add("id","int");

        Dataset<Row> dfInput=ss.readStream()
                .option("header", "true")
                .schema(schema)
                //hdfs://namenode:8020/input
                .csv("./input");

        Dataset<Row> dfOutput=dfInput.where("note>15");

        StreamingQuery query = dfOutput.writeStream()
                .format("console")
                .outputMode("append")   // ✅ append instead of complete
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("5 seconds")) // ✅ micro-batch trigger
                .start();

        query.awaitTermination();


    }
}
