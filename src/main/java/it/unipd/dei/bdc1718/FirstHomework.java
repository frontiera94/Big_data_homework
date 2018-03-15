package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Comparator;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class FirstHomework {

    public static class Minimum implements Serializable, Comparator<Double> {
        public int compare(Double a, Double b) {
            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;}

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
            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("akka").setLevel(Level.OFF);

            SparkConf conf = new SparkConf(true)
                    .setAppName("Preliminaries");
            JavaSparkContext sc = new JavaSparkContext(conf);
            //1
            // Create a parallel collection
            JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);
            double sum = dNumbers.map((x) -> x).reduce((x, y) -> x + y);

            System.out.println("dNumbers is: " );
            for (double line : dNumbers.collect()) {
                System.out.println("*" + line);
            }
            System.out.println("The sum is " + sum);

            double arithmeticMean = sum / dNumbers.count();
            System.out.println("The arithmeticMean is " + arithmeticMean);
            //2
            JavaRDD<Double> dDiffavgs = dNumbers.map((x) -> {
                double diff = Math.abs(arithmeticMean - x);
                return diff;
            });

            //3

            System.out.println("dDiffavgs is: " );
            for (double line : dDiffavgs.collect()) {
                System.out.println("*" + line);
            }

            final double[] current_min = new double[1];

            double min = dDiffavgs.reduce((x, y)-> {
                if(x<y) {
                    current_min[0] = x;
                }
                else {
                    current_min[0] = y;
                }
                return current_min[0];

            });
            System.out.println("The dDiffavgs min is:" + min);


            //4
            double minimum = dDiffavgs.min(new Minimum());
            System.out.println("The minimum gabriele is:" + minimum);

            System.out.println("SampledNum is: ");
            JavaRDD<Double> SampledNum = dNumbers.sample(true,0.75);
            for (double line : SampledNum.collect()) {
                System.out.println("*" + line);
            }
            double SampledMin = SampledNum.min(new Minimum());
            System.out.println("SampledNum min is " + SampledMin);


            JavaPairRDD<Double, Double> dNumbersWithKeys = dNumbers.mapToPair((x) -> {
                return new scala.Tuple2<>(x, x);
            });
            System.out.println("key are:");
            for (scala.Tuple2 line : dNumbersWithKeys.collect()) {
                System.out.println("*" + line);

            }
          JavaPairRDD<Double, Double> dNumbersKeySorted= dNumbersWithKeys.sortByKey(false);
          for (scala.Tuple2 line : dNumbersKeySorted.collect()) {
            System.out.println("******" + line);
          }




            // JavaRDD <Double> dDiffavgs=dNumbers.map((x)->x).reduce((x,y) -> minimum.compare()  }
        }
}

