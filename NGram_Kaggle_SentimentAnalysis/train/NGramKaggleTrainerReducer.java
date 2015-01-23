package train;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NGramKaggleTrainerReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		String line = null;
		String[] splits = null;

		// can be changed to just have one final value
		while (values.hasNext()) {
			
			line = values.next().toString();
			System.out.println("line:"+line);
			splits = line.split("_");

			System.out.println("key:" + splits[1] + "-rating" + splits[0]);

			output.collect(new Text(splits[1]),
					new IntWritable(Integer.parseInt(splits[0])));
		}

	}

}
