package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NGramKaggleTester extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		JobConf job = new JobConf(getConf(), NGramKaggleTester.class);
		job.setJobName("NGramSentiAnalysisForKaggleData");

		job.setJarByClass(NGramKaggleTester.class);
		job.setMapperClass(NGramKaggleTestMapper.class);
		job.setReducerClass(NGramKaggleTestReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//provide the data path  for kaggle test dataset
		// if runnning through HDFS then change the input file to "args[0]" instead of "Test"
		FileInputFormat.setInputPaths(job, new Path("Test"));
		//change to "args[1]" from "TestOutput" if running from HDFS
		FileOutputFormat.setOutputPath(job, new Path("TestOutput"));

		JobClient.runJob(job);

		System.out.println("Job over");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(), new NGramKaggleTester(), args);
		System.exit(res);

	}

}
