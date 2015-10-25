package nist.cleaning.jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		outKey.set(value);
		outVal.set(value);
		context.write(outKey, outVal);
	}
}