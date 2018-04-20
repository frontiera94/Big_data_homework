package it.unipd.dei.bdc1718;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.api.java.StorageLevels;
import scala.Array;
import scala.Tuple2;


public class SecondHomeworkGabriella {

    public static void main(String[] args) throws IOException, FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf configuration =
                new SparkConf(true)
                        .setAppName("Second homework ");
        JavaSparkContext sc = new JavaSparkContext(configuration);

        JavaRDD<String> docs = sc.textFile("text-sample.txt").cache();
        long counter = docs.count();
        System.out.println(counter);
        int number_partition = 64;
        int t = (int) Math.floor(counter / number_partition); // numero di elementi per ogni partizione
        Scanner s = new Scanner(new FileReader("text-sample.txt"));
        long start = System.currentTimeMillis();
        String str;
        ArrayList<String> partitioned = new ArrayList();
        for (int j = 0; j < number_partition; j++) {

            String subdocument = "";
            for (int k = 0; k < t; k++)
            {
                str = s.nextLine();

                subdocument += str;

            }
            partitioned.add(subdocument);

        }

        JavaRDD<String> dPartitioned = sc.parallelize(partitioned);
        JavaPairRDD<String, Long> alternativo = dPartitioned
                .flatMapToPair((x) -> {             // <-- Map phase
                    ArrayList<String> checked = new ArrayList();
                    String[] tokens = x.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();


                    for (String token : tokens) {

                        if ( checked.contains(token))
                        {
                            continue;
                        }
                        else
                        {
                            for (int l=0; l<x.length();l++)

                                for (int c=0; c<x.length(); c++)
                                {
                                    boolean yes=StringUtils.compare(x,token);
                                }
                            long occurences = StringUtils.compare(x,token);
                            pairs.add(new Tuple2<>(token, occurences));
                            checked.add(token);
                        }

                    }
                    return pairs.iterator();

                }).groupByKey().mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });
        PrintWriter out = new PrintWriter(new FileWriter("output.txt"));
        for (Tuple2 line : alternativo.collect()) {

            System.out.println("*" + line);
            out.println(line);

        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end - start) + " ms");
        //create an print writer for writing to a file


        //output to the file a line


        //close the file (VERY IMPORTANT!)
        out.close();
    }
}
