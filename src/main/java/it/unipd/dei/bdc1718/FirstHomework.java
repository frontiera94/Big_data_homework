package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.io.Serializable;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public class FirstHomework {

    //interface that implements minimum function
    public static class Minimum implements Serializable, Comparator<Double> {
        public int compare(Double a, Double b) {
            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;
        }
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

        // Create a parallel collection and compute the sum of the values
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);
        double sum = dNumbers.map((x) -> x).reduce((x, y) -> x + y);

        //print values of dNumbers
        System.out.println("dNumbers is: " );
        for (double line : dNumbers.collect()) {
            System.out.println("*" + line);
        }
        System.out.println("The sum is " + sum);

        ////////////////////////////
        //compute and print the arithmetic mean
        double arithmeticMean = sum / dNumbers.count();
        System.out.println("The arithmetic mean is " + arithmeticMean);

        //compute and print the geometric mean
        double product = dNumbers.map((x) -> x).reduce((x, y) -> x * y);
        double geometricMean = Math.pow(product, 1.0/(double)dNumbers.count());
        System.out.println("The geometric mean is " + geometricMean);

        //compute and print the harmonic mean
        double invsum = dNumbers.map((x) -> 1.0/x).reduce((x, y) -> x + y);
        double harmonicMean =(double)dNumbers.count()/invsum;
        System.out.println("The harmonic mean is " + harmonicMean);

        //compute and print the variance with the arithmetic mean
        double diffsum = dNumbers.map((x) -> Math.pow((arithmeticMean - x),2.0)).reduce((x,y) -> x+y);
        double variance = diffsum/dNumbers.count();
        System.out.println("The variance with the arithmetic mean is " + variance);

        //compute and print the variance with the geometric mean
        double diffsumg = dNumbers.map((x) -> Math.pow((geometricMean - x),2.0)).reduce((x,y) -> x+y);
        double varianceg = diffsumg/dNumbers.count();
        System.out.println("The variance with the geometric mean is " + varianceg);

        //compute and print the variance with the harmonic mean
        double diffsuma = dNumbers.map((x) -> Math.pow((harmonicMean - x),2.0)).reduce((x,y) -> x+y);
        double variancea = diffsuma/dNumbers.count();
        System.out.println("The variance with the harmonic mean is " + variancea);
        ////////////////////////////

        //compute the difference between values and arithmetic mean
        JavaRDD<Double> dDiffavgs = dNumbers.map((x) -> Math.abs(arithmeticMean - x));

        //print the values of dDiffargs
        System.out.println("dDiffavgs is: " );
        for (double line : dDiffavgs.collect()) {
            System.out.println("*" + line);
        }

        //compute the minimum with reduce method
        double min = dDiffavgs.reduce((x, y)-> {
            if(x<y) {
                return x;
            }
            else {
                return y;
            }
        });
        System.out.println("The minimum with reduce function is: " + min);


        //compute the minimum using Minimum method as defined before
        double minimum = dDiffavgs.min(new Minimum());
        System.out.println("The minimum with Minimum function is: " + minimum);


        //compose key-value pair of type (value, value) to sort values in dNumbers
        JavaPairRDD<Double, Double> dNumbersWithKeys = dNumbers.mapToPair((x) -> {
            return new scala.Tuple2<>(x, x);
        });

        //sort with descending order and print the values
        System.out.println("The sorted dataset is: ");
        JavaPairRDD<Double, Double> dNumbersKeySorted= dNumbersWithKeys.sortByKey(false);
        for (scala.Tuple2 line : dNumbersKeySorted.collect()) {
            System.out.println("*" + line);
        }

        //take the maximum from the sorted values
        Double maximum = dNumbersKeySorted.first()._2;
        System.out.println("The max is: " + maximum);

        //create an enlarged dataset in which the values of the original one are repeated
        //for the purpose of calculating more significant statistics than those of the original dataset
        Random ran = new Random();
        ArrayList<Double> Numbers = new ArrayList<>();
        int m = 100;
        for (int i = 0; i < m; i++){
            double n= (double) ran.nextInt(maximum.intValue())+1;
            Numbers.add(n);
        }
        //System.out.println(Numbers);

        //count and print the number of occurrences of each value in the dataset.
        JavaRDD<Double> NumbersRDD = sc.parallelize(Numbers);
        JavaPairRDD<Double, Double> dCountOccurrences = NumbersRDD.mapToPair((x) -> {
            return new scala.Tuple2<>(x, 1.0); }).reduceByKey((x,y) -> x+y);
        System.out.println("Number of occurrences of each element is: ");
        for (scala.Tuple2 line : dCountOccurrences.collect()) {
            System.out.println("*" + line);
        }

        //compute and print the "relative frequency" of each value in the dataset
        //As we can assume the numbers are iid, the relative frequency could be considered
        //a measure of probability.
        System.out.println("Relative frequency of each element is: ");
        JavaPairRDD<Double, Double> Prob = dCountOccurrences.mapValues((x)-> x/m);
        for (scala.Tuple2 line : Prob.collect()) {
            System.out.println("*" + line);
        }

        //compute expectation of Prob
        double sExpectVal = Prob.map(tuple -> tuple._1()*tuple._2()).reduce((x,y)-> x+y);
        System.out.println("Expected value is: " + sExpectVal );

        //compute variance of Prob
        double sExpectation2 = Prob.map(tuple -> Math.pow(tuple._1(),2)*tuple._2()).reduce((x,y)-> x+y);
        double sVariance = sExpectation2 - Math.pow(sExpectVal,2);
        System.out.println("Variance is: " + sVariance );

    }
}

