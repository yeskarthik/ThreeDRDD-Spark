package com.spark.stark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import scala.Tuple2;
import scala.Tuple3;


public class ThreeDRDD implements Serializable {

    public final static String headerString="tpep_pickup_datetime,pickup_longitude,pickup_latitude";
    public final static Integer N = 71176;
    
    public static double calculateMean(Long totalTripCount, Integer n) {
        return totalTripCount/(n*1.0);
    }
    
    public static double calculateS(double mean, JavaPairRDD<String,Integer> counts, Integer n)
    {
         Long squaresSum = counts.aggregate((long)0, 
            new Function2<Long, Tuple2<String, Integer>, Long> (){

                @Override
                public Long call(Long v1, Tuple2<String, Integer> v2) throws Exception {
                    v1 += v2._2 * v2._2;
                    return v1;
                }
            
        }, new Function2<Long, Long, Long> () {

            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1+v2;
            }
            
        });
        
        double sqmean=squaresSum/n;
        double s=sqmean-(mean*mean);
        return Math.sqrt(s);
    }

    
    public static void loadData(JavaSparkContext sc, String inputPath, String outputPath) {
        
        JavaRDD<String> inputRDD = sc.textFile(inputPath);
        JavaRDD<String> filterColumnRDD = inputRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                String[] cols = v1.split(",");
                return cols[1] + "," + cols[5] + "," + cols[6];
            }                       
        });
        
        JavaRDD<String> envelopeFilter = filterColumnRDD.filter(new Function<String, Boolean>(){
            @Override
            public Boolean call(String v1) throws Exception {
                if (v1.equals(headerString)) return false;
                String[] cols = v1.split(",");
                double longitude = Double.parseDouble(cols[1]);
                double latitude = Double.parseDouble(cols[2]);
                if(longitude >-73.7 || longitude<-74.25) {
                    return false;
                }
                if(latitude<40.5 || latitude > 40.9 ) {
                    return false;
                }
                return true;
            }           
        });
        
        JavaRDD<Tuple2<String, Integer>> countRDD = envelopeFilter.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String v1) throws Exception {
                String[] cols = v1.split(",");
                double longitude = ((Double.parseDouble(cols[1]) * 100 ) % 1 == 0)
                            ? (Double.parseDouble(cols[1]) * 100 ): (Double.parseDouble(cols[1]) * 100 - 1);
                double latitude = Double.parseDouble(cols[2]) * 100;
                Integer lon = (int)longitude;
                Integer lat = (int)latitude;
                String date = Integer.toString(Integer.parseInt(cols[0].split(" ")[0].split("-")[2])- 1);
                return new Tuple2<String, Integer> (lat.toString()+","+lon.toString()+","+date, (Integer) 1);
            }           
        });
        
        JavaPairRDD<String, Integer> mapCountRDD = countRDD.groupBy(new Function<Tuple2<String, Integer>, String>(){
            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1;
            }
            
        }).mapValues(new Function<Iterable<Tuple2<String, Integer>>, Integer>() {

            @Override
            public Integer call(Iterable<Tuple2<String, Integer>> v1) throws Exception {                
                Integer sum = (Integer) 0;
                for(Tuple2<String, Integer> s : v1) {
                    sum += s._2;
                }
                return sum;
            }       
        });
        
        final Map<String, Integer> countMap = mapCountRDD.collectAsMap();
        
        final Double mean = calculateMean(countRDD.count(), N);
        final Double sVal = calculateS(mean, mapCountRDD, N);
        final Broadcast<Map<String, Integer>> bcMap = sc.broadcast(countMap);
        final Broadcast<Double> bcMean = sc.broadcast(mean);
        final Broadcast<Double> bcSVal = sc.broadcast(sVal);
        final Broadcast<Integer> bcN = sc.broadcast(N);
        
        ArrayList<Integer> myArray1=new ArrayList<Integer>();
        ArrayList<Integer> myArray2=new ArrayList<Integer>();
        ArrayList<Integer> myArray3=new ArrayList<Integer>();
        
        for(int i=4050;i<=4090;i++)
        {
            myArray1.add(i);
        }
        JavaRDD<Integer> latrdd=sc.parallelize(myArray1);
        
        for(int j=0;j<=55;j++)
        {
            myArray2.add(j-7425);
        }
        JavaRDD<Integer> lonrdd=sc.parallelize(myArray2);

        
        for(int k=0;k<=30;k++)
        {
            myArray3.add(k);
        }
        JavaRDD<Integer> timerdd=sc.parallelize(myArray3);

        JavaRDD<Tuple3<Integer, Integer, Integer>> latlontime=latrdd.cartesian(lonrdd).cartesian(timerdd)
            .map(new Function<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple3<Integer, Integer, Integer>>(){
                @Override
                public Tuple3<Integer, Integer, Integer> call(Tuple2<Tuple2<Integer, Integer>, Integer> v1)
                        throws Exception {
                    return new Tuple3<Integer, Integer, Integer>(v1._1._1, v1._1._2, v1._2);
                }                   
            });
        
        JavaRDD<Tuple2<String, Double>> zScoredRDD = 
                latlontime.map(new Function<Tuple3<Integer, Integer, Integer>, Tuple2<String, Double>>(){
                    @Override
                    public Tuple2<String, Double> call(Tuple3<Integer, Integer, Integer> v1) throws Exception {                     
                        int leftlat=v1._1();
                        int leftlon=v1._2();
                        int leftday=v1._3();
                        String row=""+leftlat+","+leftlon+","+leftday+"";
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
                                    if(bcMap.value().containsKey(key))
                                    {
                                        Integer value=bcMap.value().get(key);
                                        xintowij+=value;
                                    }
                                        
                                }
                            }
                        }
                        double numerator = xintowij-(bcMean.value()*wij);
                        double denominator = bcSVal.value()*Math.sqrt(((bcN.value()*wij)-(wij*wij))/(bcN.value()-1));                          
                        return new Tuple2<String,Double>(row, numerator/denominator);
                    }           
                });
        
        JavaRDD<String> sortedResult = zScoredRDD.sortBy(new Function<Tuple2<String, Double>, Double>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call( Tuple2<String, Double> value ) throws Exception {
              return value._2;
            }
          }, false, 1 )
                .map(new Function<Tuple2<String, Double>, String>() {
            @Override
            public String call(Tuple2<String, Double> v1) throws Exception {
                return v1._1 + "," + v1._2.toString();
            }           
        });
        
        JavaRDD<String> top50 = sc.parallelize(sortedResult.take(50));
        top50.coalesce(1, true).saveAsTextFile(outputPath);        
        
    }
    

    public static void main(String[] args) throws IOException, URISyntaxException {
        SparkConf conf = new SparkConf().setAppName("ThreeDRDD");
        JavaSparkContext context = new JavaSparkContext(conf);
        ThreeDRDD.loadData(context, args[0], args[1]);
    }

}
