package maxtemp.mapreduce.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import maxtemp.mapreduce.MaxTempDriver;

public class MaxTempDriverTest {

	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);

		Path input = new Path("input/ncdc/micro");
		Path output = new Path("output");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true); // delete old output
		MaxTempDriver driver = new MaxTempDriver();
		driver.setConf(conf);
		int exitCode = driver.run(new String[] { input.toString(), output.toString() });

		assertThat(exitCode, is(0));
		// checkOutput(conf, output);
	}

}
