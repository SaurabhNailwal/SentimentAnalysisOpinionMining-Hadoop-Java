package formModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NGramCornellTrainer extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		JobConf job = new JobConf(getConf(), NGramCornellTrainer.class);
		job.setJobName("NGramSentiAnalysis");

		job.setJarByClass(NGramCornellTrainer.class);
		job.setMapperClass(NGramCornellTrainerMapper.class);
		job.setReducerClass(NGramCornellTrainerReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("Train/positive"));
		FileOutputFormat.setOutputPath(job, new Path("TrainOutput"));

		JobClient.runJob(job);

		System.out.println("Job over");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(), new NGramCornellTrainer(), args);
		System.exit(res);

	}

}
