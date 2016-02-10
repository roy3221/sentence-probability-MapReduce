package Prob;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import Utils.HDFSUtil;

public class ProbComp {
	private static String RESULT = "result";
	private static String RESULT1 = "result1";
	private static String POSWORD = "poswordresut";
	private static String POS = "posresult";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		JobConf job1 = new JobConf(ProbComp.class);
		job1.setJobName("PosWord");

//		Path outputPath = new Path(RESULT);
		HDFSUtil.deletePath(new Path(RESULT));
		HDFSUtil.deletePath(new Path(POS));
		HDFSUtil.deletePath(new Path(POSWORD));
		HDFSUtil.deletePath(new Path(RESULT1));

		// phase1,output the key-pair  <i,w>, num(i,w) as value
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(POSWORD));

		job1.setMapperClass(PosWordMap.class);
		job1.setReducerClass(PosWordRed.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		if((JobClient.runJob(job1)).isSuccessful()) {
			// phase2, 
			JobConf job2 = new JobConf(ProbComp.class);
			job2.setJobName("Pos");
			FileInputFormat.addInputPath(job2, new Path(args[0]));
			FileOutputFormat.setOutputPath(job2, new Path(POS));

			job2.setMapperClass(PosMap.class);
			job2.setReducerClass(PosRed.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			JobConf job3 = new JobConf(ProbComp.class);
			if((JobClient.runJob(job2)).isSuccessful()) {
				// phase3
				
				job3.setJobName("Perc");
				FileInputFormat.addInputPath(job3, new Path(POSWORD));// input value=w+ +i+ +word+ +num(i,w)
				FileInputFormat.addInputPath(job3, new Path(POS));// input value=T+ +i+ +length
				FileOutputFormat.setOutputPath(job3, new Path(RESULT));

				job3.setMapperClass(PercMap.class);
				job3.setReducerClass(PercRed.class);

				job3.setMapOutputKeyClass(IntWritable.class);
				job3.setMapOutputValueClass(Text.class);//key=i;value==T+ +length or value=w+ +word+ +num(i,w);

				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				JobClient.runJob(job3);
				
			}
			/*if((JobClient.runJob(job3)).isSuccessful()) {
				JobConf job4 = new JobConf(ProbComp.class);
				job4.setJobName("Perc");
				FileInputFormat.addInputPath(job4, new Path(RESULT));// input value=w+ +i+ +word+ +num(i,w)
				FileInputFormat.addInputPath(job4, new Path(args[0]));// input value=T+ +i+ +length
				FileOutputFormat.setOutputPath(job4, new Path(RESULT1));

				job4.setMapperClass(SentenceMap.class);
				//job4.setMapperClass(newSMap.class);
				job4.setReducerClass(SentenceRed.class);

				job4.setMapOutputKeyClass(LongWritable.class);
				job4.setMapOutputValueClass(Text.class);//key=i;value==T+ +length or value=w+ +word+ +num(i,w);

				job4.setOutputKeyClass(Text.class);
				job4.setOutputValueClass(FloatWritable.class);
				
				JobClient.runJob(job4);
				
			}*/
		}
	}

}
