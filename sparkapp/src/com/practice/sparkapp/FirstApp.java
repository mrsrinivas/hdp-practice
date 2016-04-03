package com.practice.sparkapp;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FirstApp {

	public static void main(String[] args) {

		String logFile = "input.txt"; // Should be some file on
										// your system
		JavaSparkContext sc = new JavaSparkContext("local", "Simple App");
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = -955956704815424207L;

			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();
		// long numAs = logData.filter(s -> s.contains("a")).count();

		long numBs = logData.filter(s -> s.contains("b")).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}

}
