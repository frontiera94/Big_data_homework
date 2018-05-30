package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class G11HM4
{
    //read the input file and return an instance of class Vector
    public static Vector strToVector(String str)
    {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        //trasforms the array "data" of double into an instance of class Vector
        return Vectors.dense(data);
    }

    // given a points set P and a number of centers k
    // returns the set S of k centers computed by the Farthest-First Traversal algorith
    public static  ArrayList<Vector> kcenter(ArrayList<Vector> P, int k)
    {

        // define the set P-S
        ArrayList<Tuple2<Vector,Double>> PS =new ArrayList<Tuple2<Vector,Double>>();

        // initialize an arrayList of tuple of type<Point,distance> with distance = + infinity
        for(int i=0;i<P.size();i++)
        {
            Tuple2<Vector,Double> ne = new Tuple2<Vector,Double>( P.get(i),(double)Integer.MAX_VALUE);
            PS.add(ne);
        }
        // we pick as first center an arbitrary point (for simplicity the first element of PS)
        ArrayList<Vector> S =new ArrayList<Vector>();
        S.add(PS.get(0)._1);
        PS.remove(0);

        for(int i=1;i<k;i++)
        {
            // for each point j of P-S we compute the squared distance between the point j and the last center added to S
            for(int j= 0; j < PS.size(); j++)
            {

                Double dist=Vectors.sqdist(PS.get(j)._1(),S.get(S.size()-1));
                // compute the minimum squared distance between a point j of P-S and the set S
                if(dist<PS.get(j)._2)
                {
                    Tuple2<Vector, Double> temp = new Tuple2<>(PS.get(j)._1,dist);
                    PS.set(j,temp);
                }
            }

            // find the point m of P-S that maximizes d(m,S)
            double distmax=PS.get(0)._2;
            int i_max=0;
            for (int m=0; m < PS.size();m++)
            {
                if(PS.get(m)._2 > distmax)
                {
                    distmax = PS.get(m)._2;
                    i_max = m;
                }
            }

            // add the point m to the set of centers S
            Tuple2<Vector, Double> temp2 = PS.get(i_max);
            S.add(temp2._1);
            PS.remove(temp2);
        }

        return S; // return the set of k centers
    }

    // given a points set of points and a number of centers k
    // returns a list of k point determined by running sequential max-diversity algorithm
    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k)
    {
        final int n = points.size();
        if (k >= n)
        {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter=0; iter<k/2; iter++)
        {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++)
            {
                if (candidates[i])
                {
                    for (int j = i+1; j < n; j++)
                    {
                        if (candidates[j])
                        {
                            double d = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                            if (d > maxDist)
                            {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++)
            {
                if (candidates[i])
                {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k)
        {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
    }


    //given a JavaRDD and a integer k and numBlocks
    //returns a set of k centers
    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsrdd,Integer k, Integer numBlocks)
    {
        long start = System.currentTimeMillis();

        //for each partition it finds a set of k centers
        //using the kcenter algorithm implemented before
        JavaRDD<Vector> coreset = pointsrdd.mapPartitions((partition) -> {
            ArrayList<Vector> temp = new ArrayList<>();
            //transform JavaRDD<Vector> in ArrayList<Vector>
            while (partition.hasNext())
            {
                Vector v = partition.next();
                temp.add(v);
            }
            ArrayList<Vector> center = kcenter(temp, k);
            return center.iterator();
        });


        ArrayList<Vector> list = new ArrayList<Vector>();
        List<Vector> list_v = coreset.collect();
        list.addAll(list_v);
        /*int j = 0;
        while (j <list.size())
        {
            System.out.println(list.get(j));
            j++;
        }*/


        long end = System.currentTimeMillis();
        System.out.println("Time taken by coreset construction: " + (end-start));

        start = System.currentTimeMillis();

        //Find the k centers using the runSequential algorithm
        ArrayList<Vector> fin_centers = runSequential(list,k);
        end = System.currentTimeMillis();
        System.out.println("Time taken by the computation of final solution: " + (end-start));

        return fin_centers;

    }

    //given a set of points
    //return the average distance between all points in points list
    public static Double measure(ArrayList<Vector> pointslist)
    {
        double sum=0;
        // for every point of P compute the minimum squared distance from its closest center
        for(int i=0;i<pointslist.size();i++)
        {
            double dist = 0;
            double mindist = Double.MAX_VALUE;
            for(int j=0;j<pointslist.size();j++)
            {
                dist =  Vectors.sqdist(pointslist.get(i),pointslist.get(j));
                if(dist < mindist)
                    mindist = dist;
            }
            sum+= dist; // update the sum of squared distances

        }
        sum=sum/(2*pointslist.size());
        return sum; // return the average squared distance of a point of P from its closest center

    }

    public static void main (String[] args) throws IOException,FileNotFoundException
    {
        int numBlocks;
        int k;

        // acquire the value of number of partition and number of center
        // that I have to find
        Scanner keyboard = new Scanner(System.in);
        System.out.println("enter an integer numBlocks");
        numBlocks = (int) keyboard.nextDouble();
        System.out.println("enter an integer k");
        k = (int) keyboard.nextDouble();

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Vector> v;
        ArrayList<Vector> sol_points;
        double avg_dist;
        for(int i =0; i<1; i++){
            System.out.println("Results for document " + i);

            // reads the input points of the input document
            v = sc.textFile(args[i]).map(G11HM4::strToVector).repartition(numBlocks).cache();
            v.count();

            // use runMapReduce to find the set of final centers for one document
            sol_points = runMapReduce(v,k, numBlocks);
            // compute average distance for one document
            avg_dist = measure(sol_points);
            System.out.println("Average distance between solution points is: " + avg_dist);
        }


    }
}