package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;


public class ThirdHomework
{


    public static  ArrayList<Vector> kcenter(ArrayList<Vector> P, int k)
    {

        Double[] value=new Double[P.size()];
        Arrays.fill(value,Integer.MAX_VALUE);
        ArrayList<Double> c=new ArrayList<Double>(Arrays.asList(value));
        ArrayList<Tuple2<Vector,Double>> PS =new ArrayList<Tuple2<Vector,Double>>();

        for(int i=0;i<P.size();i++)
        {
            Tuple2<Vector,Double> ne = new Tuple2<Vector,Double>( P.get(i),c.get(i));
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








    public static void main (String[] args) throws IOException,FileNotFoundException
    {

        ArrayList<Vector> input = InputOutput.readVectorsSeq("vecs-50-10000.txt");
        //Double dist=Vectors.sqdist(input.get(9),input.get(3));
        ArrayList<Vector> centers = kcenter(input,3);
        System.out.println(centers);



    }
}

