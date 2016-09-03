package org.apache.spark.examples;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sparkstreaming.kafka.example.ApacheAccessLog;
import com.sparkstreaming.kafka.example.Functions;

import scala.Tuple2;

public class LogAnalyzer {
	public static void main(String[] args) {
		// Create a Spark Context.
		SparkConf conf = new SparkConf().setAppName("Log Analyzer").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the text file into Spark.
		if (args.length == 0) {
			System.out.println("Must specify an access logs file.");
			args = new String[] { "/Users/vinod/spark_workspace/samples/src/main/resources/apache.access.log" };
			// System.exit(-1);
		}
		String logFile = args[0];
		JavaRDD<String> logLines = sc.textFile(logFile);

		// TODO: Insert code here for processing logs.
		// Convert the text log lines to ApacheAccessLog objects and
		// cache them since multiple transformations and actions
		// will be called on the data.
		JavaRDD<ApacheAccessLog> accessLogs = logLines.map(ApacheAccessLog::parseFromLogLine).cache();

		// Calculate statistics based on the content size.
		// Note how the contentSizes are cached as well since multiple actions
		// are called on that RDD.
		JavaRDD<Long> contentSizes = accessLogs.map(ApacheAccessLog::getContentSize).cache();
		System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
				contentSizes.reduce(Functions.SUM_REDUCER) / contentSizes.count(),
				contentSizes.min(Comparator.naturalOrder()), contentSizes.max(Comparator.naturalOrder())));

		// Compute Response Code to Count.
		List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs
				.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L)).reduceByKey(Functions.SUM_REDUCER).take(100);
		System.out.println(String.format("Response code counts: %s", responseCodeToCount));

		List<String> ipAddresses = accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
				.reduceByKey(Functions.SUM_REDUCER).filter(tuple -> tuple._2() > 10).map(Tuple2::_1).take(100);
		System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

		List<Tuple2<String, Long>> topEndpoints = accessLogs.mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
				.reduceByKey(Functions.SUM_REDUCER).top(10, new ValueComparator<>(Comparator.<Long> naturalOrder()));
		System.out.println("Top Endpoints: " + topEndpoints);

		sc.stop();
	}

	private static class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable {
		private Comparator<V> comparator;

		public ValueComparator(Comparator<V> comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
			return comparator.compare(o1._2(), o2._2());
		}
	}
}
