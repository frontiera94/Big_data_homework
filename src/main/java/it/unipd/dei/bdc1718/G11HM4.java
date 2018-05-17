package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class G11HM4
{
    public static Vector strToVector(String str)
    {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

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







    public static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsrdd,Integer k, Integer numBlocks)
    {
        ArrayList<Vector> list = new ArrayList<Vector>();
        List<Vector> list_v = pointsrdd.collect();
        list.addAll(list_v);

        int i1 = (int) Math.ceil(list.size()/numBlocks);
        System.out.println(i1);
        ArrayList<ArrayList<Vector>> sublist = new ArrayList<ArrayList<Vector>>();
        int x=0;
        for(int p=0; p<numBlocks; p++)
        {
            if(list.size()>=(x+i1))
            {
                sublist.add( new ArrayList<Vector>(list.subList(x, x+i1)));
                x+=i1;
            }
            else
                sublist.add( new ArrayList<Vector>(list.subList(x, list.size())));
        }


        ArrayList<ArrayList<Vector>> centers = new ArrayList<ArrayList<Vector>>();
        for(int p=0; p<numBlocks; p++)
        {
            kcenter(sublist.get(p),k);
        }

        return sublist.get(0);

    }

    public static void main (String[] args) throws IOException,FileNotFoundException
    {
        int numBlocks = 10;
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Vector> v = sc.textFile("vecs-50-10000.txt").map(G11HM4::strToVector).repartition(numBlocks).cache();

        ArrayList<Vector> cc = runMapReduce(v, 5, numBlocks);
        System.out.println(cc.size());

    }
}
