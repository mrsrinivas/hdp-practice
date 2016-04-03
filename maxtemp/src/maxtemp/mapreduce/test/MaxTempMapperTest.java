package maxtemp.mapreduce.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import maxtemp.mapreduce.MaxTempMapper;

public class MaxTempMapperTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = null;

	@Before
	public void before() {
		mapDriver = new MapDriver<>(new MaxTempMapper());
	}

	@Test
	public void beforeClass() throws IOException {

		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader("sample.txt");
			br = new BufferedReader(fr);
			List<String> values = new ArrayList<>();

			while (br.ready()) {
				values.add(br.readLine());
			}

			System.out.println(values);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
				"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		// Temperature ^^^^^
		mapDriver.withInput(new LongWritable(0), value).withOutput(new Text("1950"), new IntWritable(-11)).runTest();
	}

	@Test
	public void ignoreMissingTempRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"
				+ "99999V0203201N00261220001CN9999999N9+99991+99999999999");
		mapDriver.withInput(new LongWritable(0), value).run();
	}

	@After
	public void after() {
		mapDriver = null;
	}
}
