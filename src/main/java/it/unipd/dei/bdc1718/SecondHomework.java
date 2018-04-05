package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.io.Serializable;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.api.java.StorageLevels;
import scala.Tuple2;


public class SecondHomework {

    public static void main(String[] args) throws IOException,FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        // Read a list of numbers from the program options
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf configuration =
                new SparkConf(true)
                        .setAppName("Second homework");
        JavaSparkContext sc = new JavaSparkContext(configuration);
        JavaRDD<String> lines = sc.textFile("text-sample.txt");
        JavaRDD<String> docs = sc.textFile("text-sample.txt").cache();
        long counter=docs.count();

        /*
        //WORDCOUNT
        long start = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcounts = docs
                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        pairs.add(new Tuple2<>(token, 1L));
                    }

                    return pairs.iterator();
                })
                .groupByKey()                       // <-- Reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }

                    return sum;
                });

        for (Tuple2 line : wordcounts.collect()) {
               System.out.println("*" + line);
        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time 0 " + (end - start) + " ms");
        */


        //IMPROVED WORDCOUNT 1
        long start = System.currentTimeMillis();
        docs=docs.repartition(7);
        System.out.print("dimensione"+docs);

        JavaPairRDD<String, Long> wordcounts1 = docs.flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        pairs.add(new Tuple2<>(token, 1L));
                    }

                    return pairs.iterator();
        })
                .groupByKey()                       // <-- Reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }

                    return sum;
                });
        // <-- Reduce phase

        for (Tuple2 line : wordcounts1.collect()) {
            System.out.println("*" + line);
        }

        long end = System.currentTimeMillis();
        System.out.println("Elapsed time 1 " + (end - start) + " ms");
        System.out.println("Press enter to finish");
        System.in.read();
    }
}