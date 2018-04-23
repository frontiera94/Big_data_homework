package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;



public class ThirdHomework
{

    public static  ArrayList<Vector> kcenter(ArrayList<Vector> P, int k)
    {

        Double[] value=new Double[P.size()];
        Arrays.fill(value,Integer.MAX_VALUE);
        ArrayList<Double> c=new ArrayList<Double>(Arrays.asList(value));
        ArrayList<Tuple2<Vector,Double>> PS =new ArrayList<Tuple2<Vector,Double>>;

        for(int i=0;i<P.size();i++)
        {
            Tuple2<Vector,Double> ne = new Tuple2<Vector,Double>( P.get(i),c.get(i));
            PS.add(ne);
        }
        ArrayList<Tuple2<Vector,Double>> S =new ArrayList<Tuple2<Vector,Double>>;
        S.add(PS.get(0));
        PS.remove(0);
        for(int i=0;i<k;i++)
        {
            for(int j= 0; j < PS.size(); j++)
            {
                Double dist=Vectors.sqdist(PS.get(j)._1(),S.get(S.size()-1)._1());
                if(dist<PS.get(j)._2)
                {
                    Tuple2<Vector, Double> temp = new Tuple2<>(PS.get(j)._1,dist);
                    PS.set(j,temp);
                }
            }

        }


    }








    public static void main (String[] args) throws IOException,FileNotFoundException
    {

        ArrayList<Vector> input = InputOutput.readVectorsSeq("vecs-50-10000.txt");
        //Double dist=Vectors.sqdist(input.get(9),input.get(3));
        Set<Vector> set = new HashSet<>(input);
        System.out.println(set);
        Double[] value=new Double[input.size()];
        Arrays.fill(value,Integer.MAX_VALUE);
        ArrayList<Double> c=new ArrayList<Double>(Arrays.asList(value));

        ArrayList<Tuple2<Vector,Double>> h= new ArrayList<Tuple2<Vector,Double>>();
        HStack
        //set.addAll(c);



    }
}

