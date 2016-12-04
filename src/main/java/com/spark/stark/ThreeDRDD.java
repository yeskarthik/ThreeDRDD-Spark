package com.spark.stark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import scala.Tuple2;


public class ThreeDRDD implements Serializable {

    private static final long serialVersionUID = 1L;

    public static double calculateMean(JavaPairRDD<String,Integer> counts,long n){
        JavaPairRDD<String,Integer> xvalue=counts.mapToPair(new PairFunction<Tuple2<String,Integer>,String,Integer>(){
            public Tuple2<String,Integer> call(Tuple2<String,Integer> input) throws Exception {
                return new Tuple2<String,Integer>("key",input._2);

            }
        });

        JavaPairRDD<String,Integer> xvaluereducerdd=xvalue.reduceByKey(new Function2<Integer,Integer,Integer> (){
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        double mean=xvaluereducerdd.first()._2;
        mean=mean/n;
        return mean;
    }

    
    public static double calculateS(double mean,JavaPairRDD<String,Integer> counts,long n)
    {
        JavaPairRDD<String,Long> xsqvalue=counts.mapToPair(new PairFunction<Tuple2<String,Integer>,String,Long>(){
            public Tuple2<String,Long> call(Tuple2<String,Integer> input) throws Exception {
                Integer val=input._2;
                return new Tuple2<String,Long>("key",(long)val*val);

            }
        });

        JavaPairRDD<String,Long> xsqvaluereducerdd=xsqvalue.reduceByKey(new Function2<Long,Long,Long> (){
            public Long call(Long a, Long b) {
                return a + b;
            }
        });

        double sqmean=xsqvaluereducerdd.first()._2/n;
        double s=sqmean-(mean*mean);
        return Math.sqrt(s);
    }

    public static JavaPairRDD<String,Double> calculateZScore(final double mean, final double s,final long n,JavaPairRDD<String,Integer> entireGrid,final Map<String,Integer> gridMap)
    {
        final Map<String,Integer> map=gridMap;
        JavaPairRDD<String,Double> resultRdd=entireGrid.mapToPair(new PairFunction<Tuple2<String,Integer>,String,Double>(){
            public Tuple2<String,Double> call(Tuple2<String,Integer> tup) throws Exception {
                String row=tup._1;
                String[] coordleft=row.split(",");
                int leftlat=Integer.parseInt(coordleft[0]);
                int leftlon=Integer.parseInt(coordleft[1]);
                int leftday=Integer.parseInt(coordleft[2]);
                double wij=0;
                double xintowij=0;
                for(int i=leftlat-1;i<=leftlat+1;i++)
                {
                    if(i<4050 || i > 4090)
                        continue;
                    for(int j=leftlon-1;j<=leftlon+1;j++)
                    {
                        if(j >-7370 || j<-7425)
                            continue;
                        for(int k=leftday-1;k<=leftday+1;k++)
                        {
                            if(k<0 || k>30)
                                continue;
                            String key=""+i+","+j+","+k+"";
                            wij+=1;
                            if(map.containsKey(key))
                            {
                                Integer value=map.get(key);
                                xintowij+=value;
                            }

                        }
                    }
                }
                double numerator=xintowij-(mean*wij);
                double denominator=s*Math.sqrt(((n*wij)-(wij*wij))/(n-1));          
                double result=numerator/denominator;
                return new Tuple2<String,Double>(row,result);
            }
        });
        return resultRdd;
    }

    public static JavaRDD loadData(final JavaSparkContext jsc,String path, String outputPath) {
        JavaRDD<String> lines=jsc.textFile(path).map(
                new Function<String,String> () {
                    public String call(String line) throws Exception {
                        String [] parts=line.split(",");                        
                        return parts[5]+","+parts[6]+","+parts[1];
                    }
                }           
                );

        String headerString="pickup_longitude,pickup_latitude,tpep_pickup_datetime";
        JavaRDD<String> header = jsc.parallelize(Arrays.asList(headerString));
        lines=lines.subtract(header);   
        lines=lines.filter(new Function<String,Boolean>(){
            public Boolean call(String s) throws Exception {
                String [] parts=s.split(",");
                double longitude=Double.parseDouble(parts[0]);
                double latitude=Double.parseDouble(parts[1]);
                if(longitude >-73.7 || longitude<-74.25)
                {
                    return false;
                }
                if(latitude<40.5 || latitude > 40.9 )
                {
                    return false;
                }
                return true;
            }
        }
                );

        //lines.saveAsTextFile("hdfs://dds-master:54310/output/results8");


        JavaPairRDD<String,Integer> groupMap=lines.mapToPair(
                new PairFunction<String,String,Integer>(){
                    public Tuple2<String,Integer> call(String input) throws Exception {
                        String [] parts=input.split(",");
                        String date=parts[2].split(" ")[0].split("-")[2];
                        int dateInt = Integer.parseInt(date) - 1;
                        date = Integer.toString(dateInt);
                        StringBuilder sb=new StringBuilder();
                        String latitude=parts[1];
                        //40.54 40.546 
                        //sb.append("(");
                        double lat=Double.parseDouble(latitude) * 100;
                        int latInt = (int) lat;
                        sb.append(Integer.toString(latInt));
                        sb.append(",");
                        String longitude=parts[0];
                        double longit = Double.parseDouble(longitude) * 100;                        
                        int longInt = (int) longit;
                        if(longit%1!=0) {
                            longInt=longInt-1;
                        }
                        sb.append(Integer.toString(longInt));
                        sb.append(",");
                        sb.append(date);
                        //sb.append(")");
                        return new Tuple2<String,Integer>(sb.toString(),1);
                    }
                }
                );

        JavaPairRDD<String,Integer> counts=groupMap.reduceByKey(new Function2<Integer,Integer,Integer> (){

            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        final Map<String,Integer> countMap= counts.collectAsMap();

        ArrayList<String> myArray=new ArrayList<String>();
        for(int i=4050;i<=4090;i++)
        {
            myArray.add(Integer.toString(i));

        }
        JavaRDD<String> latrdd=jsc.parallelize(myArray);
        myArray=new ArrayList<String>();
        for(int j=0;j<=55;j++)
        {
            myArray.add(Integer.toString(j-7425));
        }
        JavaRDD<String> lonrdd=jsc.parallelize(myArray);

        myArray=new ArrayList<String>();
        for(int k=0;k<=30;k++)
        {
            myArray.add(Integer.toString(k));
        }
        JavaRDD<String> timerdd=jsc.parallelize(myArray);

        JavaPairRDD<String,String> latlon=latrdd.cartesian(lonrdd);
        JavaRDD<String> latlonrdd=latlon.map(new Function<Tuple2<String,String>,String>(){
            public String call(Tuple2<String,String> tup) throws Exception {
                return tup._1+","+tup._2;
            }
        });
        System.out.println(latlonrdd.count());

        JavaPairRDD<String,String> latlontime=latlonrdd.cartesian(timerdd);
        JavaRDD<String> latlontimerdd=latlontime.map(new Function<Tuple2<String,String>,String>(){
            public String call(Tuple2<String,String> tup) throws Exception {
                return tup._1+","+tup._2;
            }
        });

        long n=latlontimerdd.count();

        System.out.println(latlontimerdd.count());
        final int disp=0;

        JavaPairRDD<String,Integer> entireGrid=latlontimerdd.mapToPair(
                new PairFunction<String,String,Integer>(){
                    public Tuple2<String,Integer> call(String input) throws Exception {
                        if (countMap.containsKey(input))
                        {
                            return new Tuple2<String,Integer>(input,countMap.get(input));
                        }
                        return new Tuple2<String,Integer>(input,disp);

                    }
                });

        double mean=calculateMean(entireGrid, n);
        System.out.println("Mean is");
        System.out.println(mean);


        double svalue=calculateS(mean, entireGrid, n);
        System.out.println("Svalue is");
        System.out.println(svalue);


        JavaPairRDD<String,Double> results=calculateZScore(mean, svalue, n, entireGrid, countMap);

         JavaPairRDD<Double, String> swappedPair = results.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
             @Override
             public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
                 return item.swap();
             }

         });

         JavaPairRDD<Double, String> finalResultsRDD = swappedPair.sortByKey(false);
         JavaPairRDD<String, Double> finalResultsRDDswapped = finalResultsRDD.mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {
             @Override
             public Tuple2<String, Double> call(Tuple2<Double, String> item) throws Exception {
                 return item.swap();
             }
         });



        List<Tuple2<String, Double>> finalList = finalResultsRDDswapped.takeOrdered(50, new TupleComparator());

        JavaPairRDD<String, Double> finalResults = jsc.parallelizePairs(finalList);
        JavaRDD newRDD = finalResults.map(new Function<Tuple2<String,Double>, String>() {
            @Override
            public String call(Tuple2<String, Double> item) throws Exception {
                String first = item._1;
                String second = item._2.toString();
                return first + ", " + second;
            }
        });
        newRDD.coalesce(1, true).saveAsTextFile(outputPath);
        return newRDD;

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ThreeDRDD");
        JavaSparkContext context = new JavaSparkContext(conf);
        ThreeDRDD.loadData(context, args[0], args[1]);
    }

}

class TupleComparator implements Comparator<Tuple2< String,Double>>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Tuple2< String,Double> tuple1, Tuple2< String,Double> tuple2) {
        return tuple1._2 < tuple2._2 ? 0 : 1;
    }
}
