import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class SparkTraining {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparktraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> Rd1= sc.parallelize(Arrays.asList(1,2,3));
        System.out.println(Rd1.collect());

        System.out.println("Distinct Transformation");

        System.out.println(Rd1.distinct().collect());

        JavaRDD<Integer> Rd2=Rd1.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer x) throws Exception {
                return x*x*x;
            }
        });

        System.out.println(Rd2.collect());

        JavaRDD<Integer> OdRd=Rd1.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer i) throws Exception {
                return (i>2);
            }
        });

        System.out.println(OdRd.collect());

        JavaRDD<String> StRD= sc.parallelize(Arrays.asList("Good Morning Bharat","Have a nice day"));
        JavaRDD<String> StRD2=StRD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words=s.split(" ");
                ArrayList<String> strList=new ArrayList<>();
                for (String word : words)
                {
                    strList.add(word);
                }

                return strList.iterator();
            }
        });

        System.out.println(StRD2.collect());

        System.out.println("Distinct On RDD");

        System.out.println(StRD2.collect());

        System.out.println(OdRd.collect());
    }
}