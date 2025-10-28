package com.example.dataFramesSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Exercice2 {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Exercice2")
                .master("local[*]")
                .getOrCreate();

        final String URL = "jdbc:mysql://localhost:3306/db_hopital";
        final String USER = "root";
        final String PASSWORD ="";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", USER);
        connectionProperties.put("password", PASSWORD);
        connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> df_medecins = sparkSession.read()
                .jdbc(URL,"medecins",connectionProperties);

        Dataset<Row> df_consultations = sparkSession.read()
                .jdbc(URL,"consultations",connectionProperties);

        Dataset<Row> df_patients = sparkSession.read()
                .jdbc(URL,"patients",connectionProperties);

        //- Afficher le nombre de consultations par jour.
        df_consultations.groupBy("date_consultation")
                .agg(count(col("id"))).show();

        //Afficher le nombre de consultation par médecin. Le format d’affichage est le suivant :
        //NOM | PRENOM | NOMBRE DE CONSULTATION
        df_consultations.groupBy("id_medecin").agg(count(col("id")).alias("NOMBRE DE CONSULTATION"))
                .join(df_medecins,df_consultations.col("id_medecin").equalTo(df_medecins.col("id")))
                .select(col("nom").alias("NOM"),
                        col("prenom").alias("PRENOM"),
                        col("NOMBRE DE CONSULTATION"))
                .show();

        //- Afficher pour chaque médecin, le nombre de patients qu’il a assisté.
        df_consultations.groupBy("id_medecin").agg(countDistinct(col("id_patient"))).show();


    }
}
