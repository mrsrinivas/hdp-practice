package maxtemp.mapreduce.test;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import maxtemp.mapreduce.MaxTempReducer;

public class MaxTempReducerTest {

	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = null;

	@Before
	public void before() {
		reduceDriver = new ReduceDriver<>(new MaxTempReducer());
	}

	@Test
	public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
		reduceDriver.withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
				.withOutput(new Text("1950"), new IntWritable(10)).runTest();
	}
}
