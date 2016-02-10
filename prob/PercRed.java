package Prob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import Utils.DelimiterUtil;

public class PercRed extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {
	private Text outputValue = new Text();
	//INPUT:  key=i;value==T+ +N or value=w+ +word+ +num(i,w);
	@Override
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
//		Map<Integer, Integer> total = new HashMap<Integer, Integer>();
//		Map<Integer, Map<String, Integer>> poswd = new HashMap<Integer, Map<String, Integer>>();
		Map<String, Integer> poswd = new HashMap<String, Integer>();
		long freqt = 0;
		while (values.hasNext()) {
			Text value = values.next();
			String fields[] = DelimiterUtil.DELIMITER_PATTERN.split(value.toString());

			if (fields[0].compareTo(DelimiterUtil.POSW) == 0) {
				String word = fields[1];
				if (poswd.containsKey(word)) {
					Integer freq = poswd.get(word);
					freq += Integer.parseInt(fields[2]);
					poswd.put(word, freq);
				} else
					poswd.put(word, Integer.parseInt(fields[2]));
			} else if (fields[0].compareTo(DelimiterUtil.POST) == 0) {
				freqt += Integer.parseInt(fields[1]);
//				System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + freqt);
			}
		}
		Set<Entry<String, Integer>> poswds = poswd.entrySet();
		Iterator<Entry<String, Integer>> poswdi = poswds.iterator();
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + freqt);
		while (poswdi.hasNext()) {
			String statPair = new String();
			Entry<String, Integer> wrfreq = poswdi.next();
//			statPair += wrfreq.getKey();
			statPair += DelimiterUtil.MARK;
			statPair += DelimiterUtil.DELIMITER;
			statPair += key;
			statPair += DelimiterUtil.DELIMITER;
			statPair += wrfreq.getKey();
			statPair += DelimiterUtil.DELIMITER;
			float frq = (float)((float)wrfreq.getValue() / (float)freqt);
			statPair += frq;
			outputValue.set(statPair);
			output.collect(null, outputValue);//output=M+ +i+ +w+ +frq
		}
	}

}
