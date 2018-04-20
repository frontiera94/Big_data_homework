package it.unipd.dei.bdc1718;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class ThirdHomework
{


    public static  ArrayList<Vector> kcenter(ArrayList<Vector> P, int k)
    {
        Double[] value=new Double[P.size()];
        Arrays.fill(value,Integer.MAX_VALUE);
        ArrayList<Double> c=new ArrayList<Double>(Arrays.asList(value));
        ArrayList<Tuple2<Vector,Double>> Vmin_dist=new ArrayList<Tuple2<Vector,Double>>(P,c);

    }








    public static void main (String[] args) throws IOException,FileNotFoundException
    {

        ArrayList<Vector> input = InputOutput.readVectorsSeq("vecs-50-10000.txt");
        //Double dist=Vectors.sqdist(input.get(9),input.get(3));



    }
}

