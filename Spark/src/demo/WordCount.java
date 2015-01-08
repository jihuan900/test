package demo;

import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
 

import java.util.Arrays;
import java.util.List;

public class WordCount {
	public static void main(String[] args) {
		JavaSparkContext ctx = new JavaSparkContext("http://ubuntu-200.lubansoft.com:8888", "WordCount",
				"/home/jihuan/spark", "/home/jihuan/WordCount.jar");
		JavaRDD<String> lines = ctx.textFile("/home/jihuan/nie.txt", 1);
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	        public Iterable<String> call(String s) {
	            return Arrays.asList(s);
	        }
	    });
	    System.out.println("====words:"+words.count());
	 
	    JavaPairRDD<String, Integer> ones = words.map(new PairFunction<String, String, Integer>() {
	        public Tuple2<String, Integer> call(String s) {
	            return new Tuple2<String, Integer>(s, 1);
	        }
	    });
	 
	    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
	        public Integer call(Integer i1, Integer i2) {
	            return i1 + i2;
	        }
	    });
	    System.out.println("====½á¹û:"+counts.count());
	 
	 
//	    List<Tuple2<String, Integer>> output = counts.collect();
//	    for (Tuple2 tuple : output) {
//	        System.out.println(tuple._1 + ": " + tuple._2);
//	    }
	    System.exit(0);
	}
}
