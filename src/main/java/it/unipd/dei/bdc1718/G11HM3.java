package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import scala.Tuple3;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;


public class G11HM3
{

    // KCENTER METHOD USING FARTHEST-FIRST TRAVERSAL ALGORITHM

    //Input:
    // P = set of points
    // k = number of centers

    //Complexity:
    //O(|P|+|P-S|*k+ |P-S|*k)~ O( |P|+|P|*k)~ O(|P|*k)



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

    // K-MEANSPP
    //Input:
    //P = set of points
    // WP = set of weight for P
    // k = number of cluster


    //Complexity:
    //O( |P|+|P-S|*k+ |P-S|*k)~ O( |P|+|P|*k)~ O(|P|*k)

    public static  ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k) {
        // create an arrayList of Tuple3 of type<point,weight,distance from S>
        ArrayList<Tuple3<Vector, Long, Double>> PS = new ArrayList<Tuple3<Vector, Long, Double>>();
        for (int i = 0; i < P.size(); i++) {
            // initialize all the distances equal to + infinity
            Tuple3<Vector, Long, Double> ne = new Tuple3<Vector, Long, Double>(P.get(i), WP.get(i), (double) Integer.MAX_VALUE);
            PS.add(ne);
        }

        // choose a random point from P with uniform probability
        Random ran = new Random();
        int i_chosen = ran.nextInt(P.size());
        ArrayList<Vector> S = new ArrayList<Vector>();
        S.add(PS.get(i_chosen)._1());
        PS.remove(i_chosen);


        for (int i = 1; i < k; i++) {
            ArrayList<Double> prob = new ArrayList<>(PS.size());
            double sum_dist = 0;
            // for each point j of P-S we compute the squared distance between the point j and the last center added to S
            for (int j = 0; j < PS.size(); j++) {
                Double dist = Vectors.sqdist(PS.get(j)._1(), S.get(S.size() - 1));
                // compute the minimum squared distance between a point j of P-S and the set S
                if (dist < PS.get(j)._3()) {
                    Tuple3<Vector, Long, Double> temp = new Tuple3<>(PS.get(j)._1(), PS.get(j)._2(), dist);
                    PS.set(j, temp);
                }
                sum_dist += PS.get(j)._2() * PS.get(j)._3();
            }
            //for every point in P-S compute the probability of being chosen as next center

            for (int t = 0; t < PS.size(); t++) {
                double pp = (PS.get(t)._2() * (Math.pow(PS.get(t)._3(), 2))) / sum_dist; //vector of probabilities
                prob.add(pp);
            }
            Random rand = new Random();
            double x = rand.nextDouble(); //draw a random number x between 0 and 1
            double sum = 0;
            int save = 0;
            //selection of the index of the element to be added to S
            for (int j = 0; j < prob.size(); j++) {
                sum = sum + prob.get(j);
                if (x <= sum) {
                    save = j;
                    break;
                }
            }
            // add the point to the set of centers S
            Tuple3<Vector, Long, Double> neww = PS.get(save);
            S.add(neww._1());
            PS.remove(neww);
        }
        return S; // return the set of k centers
    }

    // K-MEANSOBJ
    //Input:
    //P = set of points
    // C = set of centers of P

    //Complexity:
    //O( |P|*|C|)


    public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C)
    {
        double sum=0;
        // for every point of P compute the minimum squared distance from its closest center
        for(int i=0;i<P.size();i++)
        {
            double dist = 0;
            double mindist = Double.MAX_VALUE;
            for(int j=0;j<C.size();j++)
            {
                dist =  Vectors.sqdist(P.get(i),C.get(j));
                if(dist < mindist)
                    mindist = dist;
            }
           sum+= dist; // update the sum of squared distances

        }
        sum=sum/P.size();
        return sum; // return the average squared distance of a point of P from its closest center
    }

    public static void main (String[] args) throws IOException,FileNotFoundException
    {
        //read the input file
        ArrayList<Vector> input = InputOutput.readVectorsSeq("vecs-50-10000.txt");


        int k;
        int k_1;
        //acquire the two values k and k1
        //if the values are double we approximate them
        try
        {
            Scanner keyboard = new Scanner(System.in);
            System.out.println("enter an integer k");
            k = (int) Math.round(keyboard.nextDouble());
            System.out.println("enter an integer k1, greater than before");
            k_1 = (int) Math.round(keyboard.nextDouble());
            //System.out.println(k+" "+ k_1);
        }
        //throw exception if inserted values are not int
        catch(Exception exc)
        {
            System.out.println(exc);
            System.out.println(" One of the two  inserted number is not a number");
            throw exc;
        }
        //correct the the value of k1 if it is lower than k
        if(k_1<k)
        {
            k_1=k_1+k;
            System.out.println("The insert number is lower than k, so k1 is set to k1+k");
        }


        //run k-center and compute the execution time
        long start = System.currentTimeMillis();
        ArrayList<Vector> centers = kcenter(input,k);
        long end = System.currentTimeMillis();
        System.out.println("Time to execute kcenter on k centers: " + (end-start));

        // initialize the weights to 1
        long[] weight = new long[input.size()];
        Arrays.fill(weight,1);

        ArrayList<Long> newweight = new ArrayList();
        for(int i=0;i<input.size();i++)
        {
            newweight.add((long) 1.0);
        }

        //run k-meansPP and compute the execution time
        start = System.currentTimeMillis();
        ArrayList<Vector> means = kmeansPP(input,newweight,k);
        end = System.currentTimeMillis();
        System.out.println("Time to execute kmeansPP on k centers: "+ (end-start));


        double avg = kmeansObj(input,means);
        System.out.println("Average squared distance for k-meansPP: "+ avg);

        //run k-center for a larger number of centers
        ArrayList<Vector> centers2 = kcenter(input,k_1);
        //initialize the weights to 1
        ArrayList<Long> newweight2 = new ArrayList();
        for(int i=0;i<centers2.size();i++)
        {
            newweight2.add((long)1.0);
        }
        //run k-meansPP on the coreset of centers computed before
        ArrayList<Vector> means2 = kmeansPP(centers2,newweight2,k);


        double avg2 = kmeansObj(input,means2);
        System.out.println("Average squared distance for k-meansPP on the coreset: "+ avg2);

        //We expected that kmeansObj worked better with k-meansPP on coreset than k-meansPP
        // but trying many values of k and k_1, we observed that
        // it is true only for very high values of k (the centers' number).
    }
}

