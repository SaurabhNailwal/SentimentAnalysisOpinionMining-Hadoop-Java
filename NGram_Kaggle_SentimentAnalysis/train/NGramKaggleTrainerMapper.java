package train;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NGramKaggleTrainerMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private Text outValue = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// System.out.println("key : " + key + "\nValue : " + value);

		String[] items = value.toString().split("\t");
		// System.out.println("split[0]" + items[0]);
		String line = "", phraseId = "";
		int rating = 0;

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
			rating = Integer.parseInt(items[3]);

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
			System.out.println("length:" + length + " line:" + line.trim());

			if (rating == 0) {

				// for rating 0

				if (length > 2) {
					// Getting triplets
					for (int i = 0; i < length - 2; i++) {
						for (int j = i + 1; j < length - 1; j++) {
							for (int k = j + 1; k < length; k++) {
								outValue.set("0_" + splits[i] + " " + splits[j]
										+ " " + splits[k]);

								output.collect(new Text(phraseId), outValue);
							}
						}
					}

					// Getting the Doublets
					for (int i = 0; i < length - 1; i++) {
						for (int j = i + 1; j < length; j++) {
							outValue.set("0_" + splits[i] + " " + splits[j]);

							output.collect(new Text(phraseId), outValue);
						}
					}
				} else {
					if (length == 2) {
						outValue.set("0_" + splits[0] + " " + splits[1]);
						output.collect(new Text(phraseId), outValue);
					} else if(splits[0].length() != 0) {

						outValue.set("0_" + splits[0]);
						output.collect(new Text(phraseId), outValue);
					}
				}

			} else if (rating == 1) {

				// for rating 1

				if (length > 2) {
					// Getting triplets
					for (int i = 0; i < length - 2; i++) {
						for (int j = i + 1; j < length - 1; j++) {
							for (int k = j + 1; k < length; k++) {
								outValue.set("1_" + splits[i] + " " + splits[j]
										+ " " + splits[k]);

								output.collect(new Text(phraseId), outValue);
							}
						}
					}

					// Getting the Doublets
					for (int i = 0; i < length - 1; i++) {
						for (int j = i + 1; j < length; j++) {
							outValue.set("1_" + splits[i] + " " + splits[j]);

							output.collect(new Text(phraseId), outValue);
						}
					}
				} else {
					if (length == 2) {
						outValue.set("1_" + splits[0] + " " + splits[1]);
						output.collect(new Text(phraseId), outValue);
					} else if(splits[0].length() != 0) {
						outValue.set("1_" + splits[0]);
						output.collect(new Text(phraseId), outValue);
					}
				}

			} else if (rating == 2) {

				// for rating 2

				if (length > 2) {
					// Getting triplets
					for (int i = 0; i < length - 2; i++) {
						for (int j = i + 1; j < length - 1; j++) {
							for (int k = j + 1; k < length; k++) {
								outValue.set("2_" + splits[i] + " " + splits[j]
										+ " " + splits[k]);

								output.collect(new Text(phraseId), outValue);
							}
						}
					}

					// Getting the Doublets
					for (int i = 0; i < length - 1; i++) {
						for (int j = i + 1; j < length; j++) {
							outValue.set("2_" + splits[i] + " " + splits[j]);

							output.collect(new Text(phraseId), outValue);
						}
					}
				} else {
					if (length == 2) {
						outValue.set("2_" + splits[0] + " " + splits[1]);
						output.collect(new Text(phraseId), outValue);
					} else if(splits[0].length() != 0){
						outValue.set("2_" + splits[0]);
						output.collect(new Text(phraseId), outValue);
					}
				}

			} else if (rating == 3) {

				// for rating 3

				if (length > 2) {
					// Getting triplets
					for (int i = 0; i < length - 2; i++) {
						for (int j = i + 1; j < length - 1; j++) {
							for (int k = j + 1; k < length; k++) {
								outValue.set("3_" + splits[i] + " " + splits[j]
										+ " " + splits[k]);

								output.collect(new Text(phraseId), outValue);
							}
						}
					}

					// Getting the Doublets
					for (int i = 0; i < length - 1; i++) {
						for (int j = i + 1; j < length; j++) {
							outValue.set("3_" + splits[i] + " " + splits[j]);

							output.collect(new Text(phraseId), outValue);
						}
					}
				} else {
					if (length == 2) {
						outValue.set("3_" + splits[0] + " " + splits[1]);
						output.collect(new Text(phraseId), outValue);
					} else if(splits[0].length() != 0) {
						outValue.set("3_" + splits[0]);
						output.collect(new Text(phraseId), outValue);
					}
				}

			} else if (rating == 4) {

				// for rating 4

				if (length > 2) {
					// Getting triplets
					for (int i = 0; i < length - 2; i++) {
						for (int j = i + 1; j < length - 1; j++) {
							for (int k = j + 1; k < length; k++) {
								outValue.set("4_" + splits[i] + " " + splits[j]
										+ " " + splits[k]);

								output.collect(new Text(phraseId), outValue);
							}
						}
					}

					// Getting the Doublets
					for (int i = 0; i < length - 1; i++) {
						for (int j = i + 1; j < length; j++) {
							outValue.set("4_" + splits[i] + " " + splits[j]);

							output.collect(new Text(phraseId), outValue);
						}
					}
				} else {
					if (length == 2) {
						outValue.set("4_" + splits[0] + " " + splits[1]);
						output.collect(new Text(phraseId), outValue);
					} else if(splits[0].length() != 0) {
						outValue.set("4_" + splits[0]);
						output.collect(new Text(phraseId), outValue);
					}
				}

			}

		}

	}

}
