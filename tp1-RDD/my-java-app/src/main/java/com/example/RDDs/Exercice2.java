package com.example.RDDs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Exercice2 {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Exercice2").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("access.log");

        // Filtrer les lignes invalides (sans guillemets ou trop courtes)
        JavaRDD<String> validLines = lines.filter(line -> line.contains("\"") && line.split(" ").length >= 9);

        // Mapper les lignes en objets LogEntry
        JavaRDD<LogEntry> logEntries = validLines.map(line -> {
            String ip = line.split(" ")[0];

            String dateTime = "";
            try {
                dateTime = line.substring(line.indexOf("[") + 1, line.indexOf("]"));
            } catch (Exception e) {
                // garder vide si malformé
            }

            String method = "";
            String resource = "";
            try {
                String requestPart = line.substring(line.indexOf("\"") + 1, line.lastIndexOf("\""));
                String[] requestParts = requestPart.split(" ");
                if (requestParts.length >= 2) {
                    method = requestParts[0];
                    resource = requestParts[1];
                }
            } catch (Exception e) {
                // garder vide si malformé
            }

            // Partie après le dernier guillemet : code HTTP et taille
            String[] partsAfterQuote = line.substring(line.lastIndexOf("\"") + 1).trim().split(" ");
            int code = partsAfterQuote.length > 0 ? safeParseInt(partsAfterQuote[0]) : 0;
            int size = partsAfterQuote.length > 1 ? safeParseInt(partsAfterQuote[1]) : 0;

            return new LogEntry(ip, dateTime, method, resource, code, size);
        });

        // Nombre total de requêtes
        long totalRequests = logEntries.count();

        // Nombre total d’erreurs (code >= 400)
        long totalErrors = logEntries.filter(entry -> entry.code >= 400).count();

        double errorPercentage = totalRequests > 0 ? ((double) totalErrors / totalRequests) * 100 : 0;

        System.out.println("Total requests: " + totalRequests);
        System.out.println("Total errors: " + totalErrors);
        System.out.println("Error percentage: " + errorPercentage + "%");

        // Top 5 IPs
        JavaPairRDD<String, Integer> ipCounts = logEntries
                .mapToPair(entry -> new Tuple2<>(entry.ip, 1))
                .reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> top5IPs = ipCounts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(5);

        System.out.println("Top 5 IPs:");
        top5IPs.forEach(System.out::println);

        // Top 5 ressources
        JavaPairRDD<String, Integer> resourceCounts = logEntries
                .mapToPair(entry -> new Tuple2<>(entry.resource, 1))
                .reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> top5Resources = resourceCounts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(5);

        System.out.println("Top 5 resources:");
        top5Resources.forEach(System.out::println);

        // Répartition des codes HTTP
        JavaPairRDD<Integer, Integer> codeCounts = logEntries
                .mapToPair(entry -> new Tuple2<>(entry.code, 1))
                .reduceByKey(Integer::sum);

        System.out.println("HTTP codes distribution:");
        codeCounts.collect().forEach(System.out::println);

        sparkContext.close();
    }

    // Méthode sécurisée pour parser un entier
    private static int safeParseInt(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // Classe pour stocker les logs
    static class LogEntry {
        String ip;
        String dateTime;
        String method;
        String resource;
        int code;
        int size;

        public LogEntry(String ip, String dateTime, String method, String resource, int code, int size) {
            this.ip = ip;
            this.dateTime = dateTime;
            this.method = method;
            this.resource = resource;
            this.code = code;
            this.size = size;
        }
    }
}
