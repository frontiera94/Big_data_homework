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

        // Spark setup
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf configuration =
                new SparkConf(true)
                        .setAppName("Second homework");
        JavaSparkContext sc = new JavaSparkContext(configuration);

        // Read file "text-sample.txt"
        JavaRDD<String> docs = sc.textFile("text-sample.txt").cache();
        long counter = docs.count();
        System.out.println("The total number of documents in the text is: " + counter);




        //WORDCOUNT
        long start = System.currentTimeMillis(); // start timer
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
        long end = System.currentTimeMillis(); // end timer
        System.out.println("The execution time of Wordcount is: " + (end - start) + " ms");






        // In order to reduce the execution time of wordcount we use the method seen in class.
        // In the map phase we do a partial count of occurences of words for each document.


        //IMPROVED WORDCOUNT 1
        docs = sc.textFile("text-sample.txt").cache(); // Read file "text-sample.txt"
        //JavaRDD<String> doc1 = docs.repartition(16);
        docs.count();
        start = System.currentTimeMillis(); // start timer
        JavaPairRDD<String, Long> wordcounts1 = docs

                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    // create an Hashset of key-value pairs of type (word, (word, # of occurences of the word in a document))
                    HashMap<String, Tuple2<String, Long>> pairs = new HashMap<>();
                    for (String token : tokens) {

                        // if the word is not already in the hashset: add new key-value pair (word,1)
                        if(!pairs.containsKey(token)){
                            Tuple2<String, Long> tuple = new Tuple2<String,Long>(token, 1L);
                            pairs.put(token, tuple);
                        }
                        //if the word is already in the hashset: increase by one the number of occurences
                        else{
                            Tuple2<String, Long> tuple = pairs.get(token);
                            tuple = new Tuple2<>(token, tuple._2() +1);
                            pairs.put(token, tuple);
                        }
                        // update the hashset

                    }


                    // save the values of the hashset in an array with key-value pair of type (word,final number of occurences in a document)
                    // to return an iterator
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
        end = System.currentTimeMillis(); // end timer
        System.out.println("The execution time of improved Wordcount 1 is: " + (end - start) + " ms");




        // To solve the problem of huge numbers of small documents we implemented wordcount in two rounds
        //using partioning



        //IMPROVED WORDCOUNT 2
        docs = sc.textFile("text-sample.txt").cache(); // Read file "text-sample.txt"
        JavaRDD<String> doc2=docs.repartition(16);  // divide the document in partition
        doc2.count();
        start = System.currentTimeMillis(); // start timer
        JavaPairRDD<String, Long> wordcounts2 = doc2
                // First round
                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    // create an Hashset of key-value pairs of type (word, ((random number,word), # of occurences of the word in a document))
                    HashMap<String, Tuple2<Tuple2<Integer,String>, Long>> pairs = new HashMap<>();
                    Random ran = new Random();
                    int key;
                    for (String token : tokens) {

                        // if the word is not already in the hashset: add new key-value pair ((random number,word),1)
                        if(!pairs.containsKey(token)){
                            Tuple2<Integer, String> ne = new Tuple2<>(key = ran.nextInt(1200) + 1,token);
                            Tuple2<Tuple2<Integer,String>, Long> tuple = new Tuple2<Tuple2<Integer,String>,Long>(ne, 1L);
                            pairs.put(token, tuple);
                        }
                        //if the word is already in the hashset: increase by one the number of occurences
                        else{
                            Tuple2<Tuple2<Integer,String>, Long> tuple = pairs.get(token);
                            Tuple2<Integer, String> ne = new Tuple2<Integer,String>(tuple._1._1(),tuple._1._2());
                            tuple = new Tuple2<Tuple2<Integer,String>,Long>(ne, tuple._2() +1);
                            pairs.put(token, tuple);
                        }
                        // update the hashset

                    }
                    // save the values of the hashset in an array with key-value pair of type ((random number,word),final number of occurences in a document)
                    // to return an iterator
                    ArrayList<Tuple2<Tuple2<Integer,String>,Long>> pairs2 = new ArrayList<>(pairs.values());


                    return pairs2.iterator();

                }).groupByKey()                 // <-- Reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (Long c : it) {
                        sum += c;
                    }
                    return sum;

                })
                // Second round
                .flatMapToPair((pair) -> {      // <-- Map phase
                    // create a new list of key-vaue pair of type (word,number of occurences)
                     ArrayList<Tuple2<String,Long>> endPair = new ArrayList();
                     // insert the result of previous round for each pair (random number,word)
                     Tuple2<String, Long> newtupla = new Tuple2<String,Long>(pair._1()._2(), pair._2());
                     endPair.add(newtupla);


                    return endPair.iterator();

                }).groupByKey()                 // <-- Reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (Long c : it) {
                        sum += c;
                    }

                    return sum;
                });

        wordcounts2.count();
        end = System.currentTimeMillis(); // end timer
        /*for (Tuple2 line : wordcounts2.collect()) {
            System.out.println("****" + line);
        }*/
        System.out.println("The execution time of improved Wordcount 2 is: " + (end - start) + " ms");






        // We modified Wordcount1 using reducebyKey instead of groupByKey and mapValues methods.
        // We used Wordcount1 because it is the fastest.


        //REDUCE BY KEY W1
        docs = sc.textFile("text-sample.txt").cache(); // Read file "text-sample.txt"
        docs.count();
        start = System.currentTimeMillis(); // start timer
        JavaPairRDD<String, Long> wordcounts3 = docs

                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    HashMap<String, Tuple2<String, Long>> pairs = new HashMap<>();
                    for (String token : tokens) {

                        // if the word is not already in the hashset: add new key-value pair (word,1)
                        if(!pairs.containsKey(token)){
                            Tuple2<String, Long> tuple = new Tuple2<String,Long>(token, 1L);
                            pairs.put(token, tuple);
                        }
                        //if the word is already in the hashset: increase by one the number of occurences
                        else{
                            Tuple2<String, Long> tuple = pairs.get(token);
                            tuple = new Tuple2<>(token, tuple._2() +1);
                            pairs.put(token, tuple);
                        }
                        // update the hashset

                    }

                    ArrayList<Tuple2<String, Long>> pairs2 = new ArrayList<>(pairs.values());
                    return pairs2.iterator();
                }).reduceByKey((x,y) -> x+y);  // <-- Reduce phase

        /*for (Tuple2 line : wordcounts4.collect()) {
            System.out.println("*" + line);
        }*/
        wordcounts3.count();
        end = System.currentTimeMillis();
        System.out.println("The execution time of Improved Wordcount 1 using Reducebykey is: " + (end - start) + " ms");







        //SEARCH FOR K MOST FREQUENT VALUES
        System.out.println("Insert the number k: ");
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
        // swap key and value in order to have the number of occurences as key.
        // Then  we sort with ascending order the new JavaPairRDD
        JavaPairRDD<Long,String> inversed=wordcounts.mapToPair((x)->(x.swap())).sortByKey(false);
        System.out.println("The "+ number + " most frequent values are: ");
        for (int j=0; j<number;j++)
        {
            Tuple2<Long,String> c=inversed.take(number).get(j);
            System.out.println("The word " + "\"" + c._2() + "\"" +  " that is repeated " + c._1() + " times");
        }
        //System.out.println(inversed.take(number));

        System.out.println("Press enter to finish");
        System.in.read();


    }}