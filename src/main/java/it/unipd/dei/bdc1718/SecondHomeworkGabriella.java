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
        int number_partition = 16;
        int t = (int) Math.floor(counter / number_partition); // numero di elementi per ogni partizione
        Scanner s = new Scanner(new FileReader("text-sample.txt"));
        ArrayList<String> subdocument = new ArrayList();
        String str;
        for (int j = 0; j < number_partition; j++) {

            for (int k = 0; k < t; k++)
            {
                str = s.nextLine();

                subdocument.add(str);
                System.out.println(subdocument.size());

            }

            JavaRDD<String> dsubdocuments = sc.parallelize(subdocument);
            JavaPairRDD<String, Long> alternativo = dsubdocuments
                    .flatMapToPair((x) -> {             // <-- Map phase
                        String[] tokens = x.split(" ");
                        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                        for (String token : tokens) {
                            pairs.add(new Tuple2<>(token, 1L));

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
            //create an print writer for writing to a file


            //output to the file a line


            //close the file (VERY IMPORTANT!)
            out.close();
        }
    }
}
