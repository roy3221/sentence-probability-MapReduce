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

public class PercMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
	private Text outputValue = new Text();
	// input value=w+ +i+ +word+ +num(i,w)
	// input value=T+ +i+ +N
	@Override
	public void  map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
			Reporter reporter) throws IOException {

		String keys[] = DelimiterUtil.DELIMITER_PATTERN.split(value.toString());
		String newValue = new String();
		newValue += keys[0];//set newValue with flag
		newValue += DelimiterUtil.DELIMITER;
		int outputKey = Integer.parseInt(keys[1]);// pass i to outputkey;
		if (keys[0].compareTo(DelimiterUtil.POSW) == 0) {
			newValue += keys[2];
			newValue += DelimiterUtil.DELIMITER;
			newValue += keys[3];
			outputValue.set(newValue);// set outputValue=w+ +word+ +num(i,w);
			output.collect(new IntWritable(outputKey), outputValue);//key=i;value=w+ +word+ +num(i,w);
		} else if (keys[0].compareTo(DelimiterUtil.POST) == 0) {
			newValue += keys[2];
			outputValue.set(newValue);//set outputValue=T+ +N;
			output.collect(new IntWritable(outputKey), outputValue);
		} else
			System.out.println("####################" + key + "!" + value + "!" + keys[0] +  "!" + keys[1]);
		System.out.println("&&&&&&&&&&&&&&&&&&&&" + outputKey + "!" + outputValue.toString() + "!" + keys[2]);
	}

}
