package nist.cleaning.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		String inputBucket = args[0];
		String outputBucket = args[1];
		Utils.inputBucket = inputBucket;
		
		Job job = Job.getInstance(conf);
		job.setInputFormatClass(NonSplitableInputFormat.class);
		NonSplitableInputFormat.setInputPaths(job, inputBucket + "/core/lane_measurements/test");
		job.setMapperClass(CalculateLaneAvgSpeedMapper.class);
		job.setReducerClass(CalculateLaneAvgSpeedReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(Driver.class);
		Path outputPath = new Path(outputBucket + "/output1");
		FileOutputFormat.setOutputPath(job, outputPath);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.submit();
		job.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(conf);
		job2.setInputFormatClass(NonSplitableInputFormat.class);
		NonSplitableInputFormat.setInputPaths(job2, outputBucket + "/output1");
		job2.setMapperClass(PreFinalMapper.class);
		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setJarByClass(Driver.class);
		outputPath = new Path(outputBucket + "/output2");
		FileOutputFormat.setOutputPath(job2, outputPath);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job2.submit();
		job2.waitForCompletion(true);
		
		
		Job job3 = Job.getInstance(conf);
		job3.setInputFormatClass(NonSplitableInputFormat.class);
		NonSplitableInputFormat.setInputPaths(job3, outputBucket + "/output2");
		job3.setMapperClass(FinalOrderingMapper.class);
		job3.setNumReduceTasks(0);
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setJarByClass(Driver.class);
		outputPath = new Path(outputBucket + "/results");
		FileOutputFormat.setOutputPath(job3, outputPath);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job3.submit();
		job3.waitForCompletion(true);
		
		
	}
}
