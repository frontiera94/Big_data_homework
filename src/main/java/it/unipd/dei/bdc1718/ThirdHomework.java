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


    public static  ArrayList<Vector> kcenter(ArrayList<Vector> P, int k)
    {

        //Double[] value=new Double[P.size()];
        //Arrays.fill(value,Integer.MAX_VALUE);
        //ArrayList<Double> c=new ArrayList<Double>(Arrays.asList(value));
        ArrayList<Tuple2<Vector,Double>> PS =new ArrayList<Tuple2<Vector,Double>>();

        for(int i=0;i<P.size();i++)
        {
            Tuple2<Vector,Double> ne = new Tuple2<Vector,Double>( P.get(i),(double)Integer.MAX_VALUE);
            PS.add(ne);
        }
        ArrayList<Vector> S =new ArrayList<Vector>();
        S.add(PS.get(0)._1);
        PS.remove(0);
        for(int i=1;i<k;i++)
        {
            for(int j= 0; j < PS.size(); j++)
            {
                Double dist=Vectors.sqdist(PS.get(j)._1(),S.get(S.size()-1));
                if(dist<PS.get(j)._2)
                {
                    Tuple2<Vector, Double> temp = new Tuple2<>(PS.get(j)._1,dist);
                    PS.set(j,temp);
                }
            }

            double max=PS.get(0)._2;
            int i_max=0;
           for (int m=0; m< PS.size();m++)
           {
               if(PS.get(m)._2>max)
               {
                   max = PS.get(m)._2;
                   i_max = m;
               }

           }

            Tuple2<Vector, Double> temp2 = PS.get(i_max);
            S.add(temp2._1);
            PS.remove(temp2);
        }

        return S;
    }

    public static  ArrayList<Vector> kmeans(ArrayList<Vector> P, ArrayList<Double> WP, int k)
    {
        ArrayList<Tuple3<Vector,Double,Double>> PS =new ArrayList<Tuple3<Vector,Double,Double>>();
        for(int i=0;i<P.size();i++)
        {
            Tuple3<Vector,Double,Double> ne = new Tuple3<Vector,Double,Double>( P.get(i),WP.get(i),(double)Integer.MAX_VALUE);
            PS.add(ne);
        }
        ArrayList<Vector> S =new ArrayList<Vector>();
        ArrayList<Double> prob = new ArrayList<>(P.size());
        S.add(PS.get(0)._1());
        PS.remove(0);

        for(int i=1;i<k;i++)
        {
            double counter = 0;
            for(int j= 0; j < PS.size(); j++)
            {
                Double dist=Vectors.sqdist(PS.get(j)._1(),S.get(S.size()-1));
                if(dist<PS.get(j)._3())
                {
                    Tuple3<Vector, Double, Double> temp = new Tuple3<>(PS.get(j)._1(),PS.get(j)._2(),dist);
                    PS.set(j,temp);
                }
                counter += Math.pow(PS.get(j)._3(),2);
            }
            for(int t = 0; t< PS.size(); t++)
            {
                double pp = (PS.get(t)._2() * (Math.pow(PS.get(t)._3(), 2))) / counter;
                prob.set(t,pp);
            }

            int start = 0;
            int[] occurences = new int[PS.size()];
            for(int e =0;e<PS.size();e++)
            {
                int number = (int) (prob.get(e) * PS.size());
                Arrays.fill(occurences,start,start+number,e);
                start += number;
            }
            Random ran = new Random();
            int index =ran.nextInt(PS.size());
            int index2 = occurences[index];
            Tuple3<Vector, Double, Double> neww = PS.get(index2);
            S.add(neww._1());
            PS.remove(neww);
        }
        return S;
    }






    public static void main (String[] args) throws IOException,FileNotFoundException
    {

        ArrayList<Vector> input = InputOutput.readVectorsSeq("vecs-50-10000.txt");
        //Double dist=Vectors.sqdist(input.get(9),input.get(3));
        ArrayList<Vector> centers = kcenter(input,3);
        System.out.println(centers);



    }
}

