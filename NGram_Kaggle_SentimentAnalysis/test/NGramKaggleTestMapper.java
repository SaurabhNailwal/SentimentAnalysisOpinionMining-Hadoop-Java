package test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NGramKaggleTestMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private Text outValue = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String[] items = value.toString().split("\t");
		String line = "";
		String phraseId = "";

		// negative - 0
		// somewhat negative - 1
		// neutral - 2
		// somewhat positive - 3
		// positive - 4

		String[] splits = null;

		// skip first line
		if (items[0].equalsIgnoreCase("PhraseId")) {
			// skip
		} else {
			line = items[2];
			phraseId = items[0];

			// removing 's
			line = line.replaceAll("'s", "");

			// remove special characters
			line = line
					.replaceAll(
							"\\'|\"|[()]|\\/|\\_|\\[|\\]|\\#|\\$|\\*|\\=|\\`|\\>|\\<|\\?|\\&|\\@|\\+|\\|\\%|\\^|\\+",
							"");

			// remove punctuations
			line = line.replaceAll("\\.|\\,|\\?|\\!|\\:|\\;|\t|\\-", " ");

			// replace all spaces
			line = line.replaceAll("\\s+|\t|\n", " ");

			splits = line.trim().toLowerCase().split("\\s+");
			int length = splits.length;

			if (length > 2) {
				// Getting triplets
				for (int i = 0; i < length - 2; i++) {
					for (int j = i + 1; j < length - 1; j++) {
						for (int k = j + 1; k < length; k++) {
							outValue.set(splits[i] + " " + splits[j] + " "
									+ splits[k]);
							output.collect(new Text(phraseId), outValue);
						}
					}
				}

				// Getting the Doublets
				for (int i = 0; i < length - 1; i++) {
					for (int j = i + 1; j < length; j++) {
						outValue.set(splits[i] + " " + splits[j]);
						output.collect(new Text(phraseId), outValue);
					}
				}
			} else {
				if (length == 2) {
					outValue.set(splits[0] + " " + splits[1]);
					output.collect(new Text(phraseId), outValue);
				} else if (splits[0].length() != 0) {

					outValue.set(splits[0]);
					output.collect(new Text(phraseId), outValue);
				}
			}

		}

	}

}
