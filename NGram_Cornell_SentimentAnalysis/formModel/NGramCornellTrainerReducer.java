package formModel;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class NGramCornellTrainerReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, IntWritable> {

	
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		String line = null;
		String[] splits = null;

		//can be changed to just have one final value
		while (values.hasNext()) {

			line = values.next().toString();	
			splits = line.split("\\s+");
			
			output.collect(new Text(line), new IntWritable(splits.length));

		}		

		
	}


}
