package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

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
        //System.out.println(counter);

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
            //   System.out.println("*" + line);
        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time 0 " + (end - start) + " ms");

        String input = null;
        int number = 0;
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
            input = bufferedReader.readLine();
            number = Integer.parseInt(input);
        } catch (NumberFormatException ex) {
            System.out.println("Not a number !");
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<Long,String> inversed=wordcounts.mapToPair((x)->(x.swap())).sortByKey(false);
        System.out.println(inversed.take(number));

        //IMPROVED WORDCOUNT 1
        start = System.currentTimeMillis();
        JavaRDD<String> doc1=docs.repartition(16);
        //System.out.println("part is" + docs.partitions.length());

        JavaPairRDD<String, Long> wordcounts1 = doc1

                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    HashMap<String, Tuple2<String, Long>> pairs = new HashMap<>();
                    for (String token : tokens) {
                        Tuple2<String, Long> tuple = pairs.get(token);
                        if(tuple==null){
                            tuple = new Tuple2<String,Long>(token, 1L);
                        }
                        else{
                            tuple = new Tuple2<>(token, tuple._2() +1);
                        }
                        pairs.put(token, tuple);
                    }

                    ArrayList<Tuple2<String, Long>> pairs2 = new ArrayList<>(pairs.values());
                    return pairs2.iterator();
                }).groupByKey()                       // <-- Reduce phase
                  .mapValues((it) -> {
                        long sum = 0;
                        for (long c : it) {
                            sum += c;
                        }

                        return sum;
                    });;  // <-- Reduce phase

        for (Tuple2 line : wordcounts1.collect()) {
            System.out.println("*" + line);
        }

/*
        //IMPROVED WORDCOUNT 2
        start = System.currentTimeMillis();
        //JavaRDD<String> doc2=docs.repartition(16);
        //System.out.println("part is" + docs.partitions.length());

        JavaPairRDD<String, Long> wordcounts2 = doc1

                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    //ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {

                        if(token==)
                        //pairs.add(new Tuple2<>(token, 1L));
                    }

                    return pairs.iterator();
                });

        //JavaPairRDD<String, Long> w1= wordcount1.countByKey();

        //wordcounts1.groupByKey()                       // <-- Reduce phase
              /*  .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }

                    return sum;
                });;  // <-- Reduce phase

        for (Tuple2 line : wordcounts2.collect()) {
            // System.out.println("*" + line);
        }

*/

        end = System.currentTimeMillis();
        System.out.println("Elapsed time 1 " + (end - start) + " ms");
        System.out.println("Press enter to finish");
        System.in.read();
    }
}