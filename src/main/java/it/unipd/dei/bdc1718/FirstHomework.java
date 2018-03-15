package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Comparator;
import java.io.Serializable;




public class FirstHomework {

    public static class Minimum implements Serializable, Comparator<Double> {
        public double compare(double a, double b) {
            if (a < b) return a;
            else if (a > b) return b;
            return a;}
        }

        public static void main(String[] args) throws FileNotFoundException {
            if (args.length == 0) {
                throw new IllegalArgumentException("Expecting the file name on the command line");
            }

            // Read a list of numbers from the program options
            ArrayList<Double> lNumbers = new ArrayList<>();
            Scanner s = new Scanner(new File(args[0]));
            while (s.hasNext()) {
                lNumbers.add(Double.parseDouble(s.next()));
            }
            s.close();

            // Setup Spark
            SparkConf conf = new SparkConf(true)
                    .setAppName("Preliminaries");
            JavaSparkContext sc = new JavaSparkContext(conf);

            // Create a parallel collection
            JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);
            double sum = dNumbers.map((x) -> x).reduce((x, y) -> x + y);

            System.out.println("The sum is " + sum);

            double arithmeticMean = sum / dNumbers.count();
            System.out.println("The arithmeticMean is " + arithmeticMean);

            JavaRDD<Double> newdNumbers = dNumbers.map((x) -> {
                double diff = Math.abs(arithmeticMean - x);
                return diff;
            });

            for (double line : newdNumbers.collect()) {
                System.out.println("*" + line);
            }
            double minumum = dNumbers.min(new Minimum());
            System.out.print(minumum);


            // JavaRDD <Double> dDiffavgs=dNumbers.map((x)->x).reduce((x,y) -> minimum.compare()  }
        }
}

