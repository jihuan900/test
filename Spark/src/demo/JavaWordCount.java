package demo;

import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

public class JavaWordCount {

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err
					.println("Usage: JavaWordCount <master> <input> <output> <numOutputFiles>");
			System.exit(1);
		}

		JavaSparkContext spark = new JavaSparkContext(args[0],
				"Java Wordcount", System.getenv("SPARK_HOME"),
				"JavaWordCount.jar");

		JavaRDD<String> file = spark.textFile(args[1]);

		Integer numOutputFiles = Integer.parseInt(args[3]);

		JavaRDD<String> words = file
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) {
						return Arrays.asList(s.toLowerCase().split("\\W+"));
					}
				});

		JavaPairRDD<String, Integer> pairs = words
				.map(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}, numOutputFiles);

		counts.sortByKey(true).saveAsTextFile(args[2]);
		System.exit(0);
	}
}
