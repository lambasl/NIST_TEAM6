package nist.cleaning.jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FinalOrderingMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	MultipleOutputs<NullWritable, Text> multipleOutput;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException,
			InterruptedException {
		multipleOutput = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] splits = value.toString().split("\\t");
		multipleOutput.write(NullWritable.get(), new Text(splits[1] + "\t" + splits[2] + "\t" +  splits[3]), splits[4]);

		
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException,
			InterruptedException {
		multipleOutput.close();
	}

}
