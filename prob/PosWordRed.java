package Prob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import Utils.DelimiterUtil;

public class PosWordRed extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		long count = 0;
		String inputKey = key.toString(); 
		while (values.hasNext()) {
//			count++;
			count += values.next().get();
		}
		inputKey += DelimiterUtil.DELIMITER;
		inputKey += count;
		Text outputValue = new Text();
		outputValue.set(inputKey);
		output.collect(null, outputValue);
	}

}
