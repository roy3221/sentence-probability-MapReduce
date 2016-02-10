package Prob;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import Utils.DelimiterUtil;

public class PosMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outputKey = new Text();

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		String words[] = DelimiterUtil.DELIMITER_PATTERN.split(value.toString());
		for (int i = 0; i < words.length; i++) {
			String newKey = new String();
			newKey += DelimiterUtil.POST;
			newKey += DelimiterUtil.DELIMITER;
			newKey += i;
			outputKey.set(newKey);
			output.collect(outputKey, new IntWritable(1));
		}
	}
}
