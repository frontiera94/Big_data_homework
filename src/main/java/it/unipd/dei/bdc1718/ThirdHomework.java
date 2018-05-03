package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import scala.Tuple3;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;


public class ThirdHomework
{

    // KCENTER METHOD USING FARTHEST-FIRST TRAVERSAL ALGORITHM

    // P = set of points
    // k = number of centers
    public static  ArrayList<Vector> kcenter(ArrayList<Vector> P, int k)
    {

        //Double[] value=new Double[P.size()];
        //Arrays.fill(value,Integer.MAX_VALUE);
        //ArrayList<Double> c=new ArrayList<Double>(Arrays.asList(value));

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
            // for each point c of P-S we compute the distance between the point and the last center added to S
            for(int j= 0; j < PS.size(); j++)
            {
                // compute the minimum distance between a point c of P-S and the set S
                Double dist=Vectors.sqdist(PS.get(j)._1(),S.get(S.size()-1));
                if(dist<PS.get(j)._2)
                {
                    Tuple2<Vector, Double> temp = new Tuple2<>(PS.get(j)._1,dist);
                    PS.set(j,temp);
                }
            }

            // find the point c of P-S that maximizes d(c,S)
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

            // add the point c to the set of centers S
            Tuple2<Vector, Double> temp2 = PS.get(i_max);
            S.add(temp2._1);
            PS.remove(temp2);
        }

        // return the set of k centers
        return S;
    }

    // KMEANSPP
    //P = set of points
    // WP = set of weight for P
    // k = number of cluster
    public static  ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k)
    {
        // create an arrayList of Tuple3 of type<point,weight,distance from S>
        ArrayList<Tuple3<Vector,Long,Double>> PS =new ArrayList<Tuple3<Vector,Long,Double>>();
        for(int i=0;i<P.size();i++)
        {
            // initialize all the distances equal to + infinity
            Tuple3<Vector,Long,Double> ne = new Tuple3<Vector,Long,Double>( P.get(i),WP.get(i),(double)Integer.MAX_VALUE);
            PS.add(ne);
        }

        // choose a random point from P with uniform probability
        Random ran = new Random();
        int i_chosen = ran.nextInt(P.size());
        ArrayList<Vector> S =new ArrayList<Vector>();
        S.add(PS.get(i_chosen)._1());
        PS.remove(i_chosen);


        for(int i=1;i<k;i++)
        {
            ArrayList<Double> prob = new ArrayList<>(PS.size());
            double counter = 0;
            for(int j= 0; j < PS.size(); j++)
            {
                Double dist=Vectors.sqdist(PS.get(j)._1(),S.get(S.size()-1));
                if(dist<PS.get(j)._3())
                {
                    Tuple3<Vector, Long, Double> temp = new Tuple3<>(PS.get(j)._1(),PS.get(j)._2(),dist);
                    PS.set(j,temp);
                }
                counter += Math.pow(PS.get(j)._3(),2);
            }
            for(int t = 0; t< PS.size(); t++)
            {
                double pp = (PS.get(t)._2() * (Math.pow(PS.get(t)._3(), 2))) / counter;
                prob.add(pp);
                //System.out.println(pp);
            }
            Random rand = new Random();
            double x =rand.nextDouble();
            //System.out.println("random " + x);
            double sum = 0;
            int save = 0;
            for(int j=0;j<prob.size();j++)
            {
                sum = sum + prob.get(j);
                if(x <= sum)
                {
                    save = j;
                    break;
                }


            }
            Tuple3<Vector, Long, Double> neww = PS.get(save);
            S.add(neww._1());
            PS.remove(neww);

        }

        return S;
    }


    public static  ArrayList<Tuple2<Vector,Double>> kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C)
    {
        ArrayList<Tuple2<Vector,Double>> end = new ArrayList<>();
        for(int i=0;i<P.size();i++)
        {
            double dist = 0;
            double mindist = Double.MAX_VALUE;
            for(int j=0;j<C.size();j++)
            {
                dist =  Vectors.sqdist(P.get(i),C.get(j))/P.size();
                if(dist < mindist)
                    mindist = dist;
            }
            Tuple2<Vector,Double> temp = new Tuple2<>(P.get(i),mindist);
            end.add(temp);
        }
        return end;
    }




    public static void main (String[] args) throws IOException,FileNotFoundException
    {

        ArrayList<Vector> input = InputOutput.readVectorsSeq("vecs-50-10000.txt");
        //Double dist=Vectors.sqdist(input.get(9),input.get(3));
        long start = System.currentTimeMillis();

        Scanner keyboard = new Scanner(System.in);
        System.out.println("enter an integer k");
        int k = keyboard.nextInt();
        System.out.println("enter an integer k1, greater than before");
        int k_1 = keyboard.nextInt();
        //cccc


        ArrayList<Vector> centers = kcenter(input,k);
        long end = System.currentTimeMillis();
        int uuuuu = centers.size();
        System.out.println(end-start);
        //System.out.println(centers);

        long[] weight = new long[input.size()];
        Arrays.fill(weight,1);
        ArrayList<Long> newweight = new ArrayList();
        for(int i=0;i<input.size();i++)
        {
            newweight.add((long)1.0);
        }
        start = System.currentTimeMillis();
        ArrayList<Vector> means = kmeansPP(input,newweight,k);
        end = System.currentTimeMillis();
        int yyyyy = means.size();
        System.out.println(end-start);
        //System.out.println(means);

        ArrayList<Tuple2<Vector,Double>> obj = kmeansObj(input,means);
        //System.out.println(obj);

        ArrayList<Vector> centers2 = kcenter(input,k_1);
        ArrayList<Long> newweight2 = new ArrayList();
        for(int i=0;i<centers2.size();i++)
        {
            newweight2.add((long)1.0);
        }
        ArrayList<Vector> means2 = kmeansPP(centers2,newweight2,k);
        System.out.println(means2);
        ArrayList<Tuple2<Vector,Double>> obj2 = kmeansObj(input,means2);
        System.out.println(obj2);


    }
}

