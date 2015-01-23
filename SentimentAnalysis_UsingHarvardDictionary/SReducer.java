package reviewComment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.Lin;

public class SReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text> {

	private Text comment = new Text();
	private Text type = new Text("neutral");
	private static HashMap<String, Integer> wordPoints = new HashMap<String, Integer>();
	private static HashMap<String, HashMap<String, Double>> notFound = new HashMap<String, HashMap<String, Double>>();

	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		String harvPos = "/home/saurabh/HarvardData/harvPositive.txt";
		String harvNeg = "/home/saurabh/HarvardData/harvNegative.txt";
		String value = "";
		StringBuilder rComment = new StringBuilder();
		
        //clear the previous values from map		
		wordPoints.clear();
		notFound.clear();
		
         while (values.hasNext()) {

			value = values.next().toString();
			//rComment.append(value);
			// comparing with Harvard Positive words
			WordComparator(harvPos, value, "positive");

			// comparing with Harvard Negative words
			WordComparator(harvNeg, value, "negative");
		}
		// to clear the words in the 'not found' list
		clearNotFound();

		int sum = 0;
		for (String keys : wordPoints.keySet()) {

			sum += wordPoints.get(keys);
		}

		if (sum > 0) {
			type.set("positive");
		} else if (sum < 0) {
			type.set("negative");
		}

		comment.set("comment#"+key + "------>>>");
		output.collect(comment, type);

	}

	public static void WordComparator(String file, String comment, String type) {

		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		String line = "";
		String[] word = comment.split("\\s+");
		HashMap<String, Double> wordRelatedness = null;

		try {

			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);
			
			while ((line = bufferedReader.readLine()) != null) {
				
				for (int i = 0; i < word.length; i++) {
					if (word[i].equalsIgnoreCase(line.trim())) {
						// other than positive or negative we should decide
						// how many points to give based on other features
						if (type.equalsIgnoreCase("positive")) {
							wordPoints.put(word[i], 2);

						} else {
							if (type.equalsIgnoreCase("negative")) {
								wordPoints.put(word[i], -2);

							}
						}

						break;

					} else {
						// if not present in either list
						// notFound.put(word[i],);
						wordRelatedness = findRelatedness(word[i], line.trim(),
								type);
						notFound.put(word[i], wordRelatedness);
					}
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (fileReader != null) {
				try {
					fileReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public static HashMap<String, Double> findRelatedness(String word,
			String dictionaryWord, String type) {

		HashMap<String, Double> wordRelatedness = new HashMap<String, Double>();

		if (notFound.get(word) != null)
			wordRelatedness = notFound.get(word);

		ILexicalDatabase lexDb = new NictWordNet();
		RelatednessCalculator rc = new Lin(lexDb); // LESK can also be
													// considered
		Double score = rc.calcRelatednessOfWords(word, dictionaryWord);

		wordRelatedness.put(dictionaryWord + "_" + type, score);
		return wordRelatedness;

	}

	public static void clearNotFound() {

		HashMap<String, Double> wordScore = null;
		for (String key : notFound.keySet()) {

			// checking if the word has been found
			// If found then no need of using relatedness
			if (wordPoints.get(key) != null)
				continue;

			wordScore = notFound.get(key);

			// sort the score values

			List<Map.Entry<String, Double>> maxScore = new LinkedList<Map.Entry<String, Double>>(
					wordScore.entrySet());

			Collections.sort(maxScore,
					new Comparator<Map.Entry<String, Double>>() {

						public int compare(Map.Entry<String, Double> map1,
								Map.Entry<String, Double> map2) {

							return (map2.getValue()).compareTo(map1.getValue());
						}

					});

			// the word with the highest score is most close
			// get the first word score after sorting if greater than 0.6

			String[] word = null;
			Double score = 0D;

			for (Iterator<Map.Entry<String, Double>> iterator = maxScore
					.iterator(); iterator.hasNext();) {

				Map.Entry<String, Double> entry = iterator.next();

				System.out.println("Word and Score - " + entry.getKey() + " "
						+ entry.getValue());

				word = entry.getKey().split("_");
				score = entry.getValue();
				// just require the first value
				break;

			}
			if (score > 0.6) {

				if (word[1].equalsIgnoreCase("positive")) {
					wordPoints.put(key, 1);
				} else {
					wordPoints.put(key, -1);
				}
				// Can add the word to the either Harvard File

			} else {
				// word wasn't in either file and not close enough to any word
				wordPoints.put(key, 0);
			}

		}

	}

}
