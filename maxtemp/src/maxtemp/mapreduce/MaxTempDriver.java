package maxtemp.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTempDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: MaxTempDriver <input path> <outputpath>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(MaxTempDriver.class);
		job.setJobName("Max Temparature MapReduce application");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		System.out.println(job.waitForCompletion(true) ? 0 : 1);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		MaxTempDriver driver = new MaxTempDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

}
