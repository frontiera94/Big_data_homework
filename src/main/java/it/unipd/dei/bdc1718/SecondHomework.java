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
        long counter = docs.count();
        System.out.println("The total number of words in the text is: " + counter);




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

                }).groupByKey()  // <-- Reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }

                    return sum;
                });
        wordcounts.count();
        /*for (Tuple2 line : wordcounts.collect()) {
               System.out.println("*" + line);
        }*/
        long end = System.currentTimeMillis();
        System.out.println("Wordcount is: " + (end - start) + " ms");


/*

        //SEARCH FOR K MOST FREQUENT VALUES
        System.out.println("Insert a number: ");
        //stamp first k value
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

*/


        //IMPROVED WORDCOUNT 1
        JavaRDD<String> doc1 =docs.repartition(16);
        start = System.currentTimeMillis();
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
                });;

        /*for (Tuple2 line : wordcounts1.collect()) {
            System.out.println("*" + line);
        }*/
        wordcounts1.count();
        end = System.currentTimeMillis();
        System.out.println("Improved Worcount 1: " + (end - start) + " ms");




        //IMPROVED WORDCOUNT 2
        JavaRDD<String> doc2=docs.repartition(16);
        start = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcounts2 = doc2

                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    HashMap<String, Tuple2<Tuple2<Integer,String>, Long>> pairs = new HashMap<>();
                    Random ran = new Random();
                    int key;
                    for (String token : tokens) {
                        Tuple2<Tuple2<Integer,String>, Long> tuple = pairs.get(token);
                        if(tuple==null){
                            Tuple2<Integer, String> ne = new Tuple2<>(key = ran.nextInt(100) + 1,token);
                            tuple = new Tuple2<Tuple2<Integer,String>,Long>(ne, 1L);

                        }
                        else{
                            Tuple2<Integer, String> ne = new Tuple2<Integer,String>(tuple._1._1(),tuple._1._2());
                            tuple = new Tuple2<Tuple2<Integer,String>,Long>(ne, tuple._2() +1);
                        }
                        pairs.put(token, tuple);
                    }
                    ArrayList<Tuple2<Tuple2<Integer,String>,Long>> pairs2 = new ArrayList();
                    pairs2.addAll(pairs.values());

                    return pairs2.iterator();

                }).groupByKey()                 // <-- First reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (Long c : it) {
                        sum += c;
                    }
                    return sum;

                }).flatMapToPair((pair) -> {      // <-- Second map phase
                    ArrayList<Tuple2<String,Long>> endPair = new ArrayList();
                    //for (Tuple2<Tuple2<Integer, String>, Long> pair : pairs ) {
                        Tuple2<String, Long> newtupla = new Tuple2<String,Long>(pair._1()._2(), pair._2());
                        endPair.add(newtupla);
                    //}

                    return endPair.iterator();

                }).groupByKey()                 // <-- Second reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (Long c : it) {
                        sum += c;
                    }

                    return sum;
                });

        wordcounts2.count();
        end = System.currentTimeMillis();
        /*for (Tuple2 line : wordcounts2.collect()) {
            System.out.println("****" + line);
        }*/
        System.out.println("Improved Wordcount 2: " + (end - start) + " ms");




        //REDUCE BY KEY W
        start = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcounts3 = docs
                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        pairs.add(new Tuple2<>(token, 1L));
                    }

                    return pairs.iterator();
                }).reduceByKey((x,y) -> x+y); // <-- Reduce phase
        wordcounts3.count();
        end = System.currentTimeMillis();
        /*for (Tuple2 line : wordcounts3.collect()) {
               System.out.println("*" + line);
        }*/

        System.out.println("Reducebykey is: " + (end - start) + " ms");




        //REDUCE BY KEY W1
        start = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcounts4 = doc1

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
                }).reduceByKey((x,y) -> x+y);  // <-- Reduce phase

        /*for (Tuple2 line : wordcounts4.collect()) {
            System.out.println("*" + line);
        }*/
        wordcounts4.count();
        end = System.currentTimeMillis();
        System.out.println("Reducebykey w1 is: " + (end - start) + " ms");




        //REDUCE BY KEY W2
        start = System.currentTimeMillis();
        JavaPairRDD<String, Long> wordcounts5 = doc2

                .flatMapToPair((document) -> {             // <--  First map phase
                    String[] tokens = document.split(" ");
                    HashMap<String, Tuple2<Tuple2<Integer,String>, Long>> pairs = new HashMap<>();
                    Random ran = new Random();
                    int key;
                    for (String token : tokens) {
                        Tuple2<Tuple2<Integer,String>, Long> tuple = pairs.get(token);
                        if(tuple==null){
                            Tuple2<Integer, String> ne = new Tuple2<>(key = ran.nextInt(100) + 1,token);
                            tuple = new Tuple2<Tuple2<Integer,String>,Long>(ne, 1L);

                        }
                        else{
                            Tuple2<Integer, String> ne = new Tuple2<Integer,String>(tuple._1._1(),tuple._1._2());
                            tuple = new Tuple2<Tuple2<Integer,String>,Long>(ne, tuple._2() +1);
                        }
                        pairs.put(token, tuple);
                    }
                    ArrayList<Tuple2<Tuple2<Integer,String>,Long>> pairs2 = new ArrayList();
                    pairs2.addAll(pairs.values());

                    return pairs2.iterator();

                }).reduceByKey((x,y) -> x+y)  // <-- First reduce phase
                .flatMapToPair((pair) -> {    // <-- Second map phase
                    ArrayList<Tuple2<String,Long>> endPair = new ArrayList();
                    //for (Tuple2<Tuple2<Integer, String>, Long> pair : pairs ) {
                    Tuple2<String, Long> newtupla = new Tuple2<String,Long>(pair._1()._2(), pair._2());
                    endPair.add(newtupla);
                    //}

                    return endPair.iterator();
                }).reduceByKey((x,y) -> x+y);  // <-- Second reduce phase


        /*for (Tuple2 line : wordcounts5.collect()) {
        System.out.println("*" + line);
        }*/
        wordcounts5.count();
        end = System.currentTimeMillis();
        System.out.println("Reducebykey w2 is: " + (end - start) + " ms");



        System.out.println("Press enter to finish");
        System.in.read();


    }}