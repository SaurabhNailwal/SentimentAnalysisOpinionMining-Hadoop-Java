package reviewComment;

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

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.Lesk;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class SentiReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text> {

	private Text comment = new Text();
	private Text type = new Text("neutral");
	private static HashMap<String, Double> wordPoints = new HashMap<String, Double>();
	private static HashMap<String, HashMap<String, Double>> notFound = new HashMap<String, HashMap<String, Double>>();

	private static HashMap<String, Double> dictionary = new HashMap<String, Double>();

	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		String value = "";

		// clear the previous values from map
		wordPoints.clear();
		notFound.clear();

		while (values.hasNext()) {

			value = values.next().toString();
			WordComparator(value);

		}

		// to clear the words in the 'not found' list
		clearNotFound();

		Double sum = 0.0;

		for (String keys : wordPoints.keySet()) {

			sum += wordPoints.get(keys);
			System.out.println(keys + " - " + wordPoints.get(keys));
		}

		if (sum > 0) {
			type.set("positive");
		} else if (sum < 0) {
			type.set("negative");
		}

		comment.set("comment#" + key + "------>>>");
		output.collect(comment, type);
	}

	public static void WordComparator(String comment) throws IOException {

		String[] word = comment.split("\\s+");
		HashMap<String, Double> wordRelatedness = new HashMap<String, Double>();

		String pathToSWN = "/home/saurabh/SentiWordNet/SentiWordNetData.txt";
		dictionary = SentiWordNet.SentiWordNetList(pathToSWN);

		for (int i = 0; i < word.length; i++) {

			// POS tagging
			String pos = "";

			// Morphology morph = new Morphology();
			//
			// MaxentTagger tagger = new
			// MaxentTagger("/home/saurabh/SentiWordNet/english-left3words-distsim.tagger");
			// //pos = tagger.lemmatize(word[i], morph);
			// pos = tagger.tagString(word[i]);
			//
			// String lemmaWord = morph.lemma(word[i], pos);
			// System.out.println("Original word:"+word[i]+" - lemmatized ="+lemmaWord);
			//
			// System.out.println("POS for "+word[i]+" : "+pos);

			// POS tagging and lemmatization
			Properties props = new Properties();
			props.put("annotators", "tokenize,ssplit,pos,lemma");

			StanfordCoreNLP stanfordNLP = new StanfordCoreNLP(props);
			Annotation document = new Annotation(word[i]);
			stanfordNLP.annotate(document);

			String lemma = "";
			List<CoreMap> sentences = document.get(SentencesAnnotation.class);

			for (CoreMap sentence : sentences) {
				// traversing the words in the current sentence
				// a CoreLabel is a CoreMap with additional token-specific
				// methods

				for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
					// this is the text of the token
					// word1 = token.get(TextAnnotation.class);
					// this is the POS tag of the token
					pos = token.get(PartOfSpeechAnnotation.class);
					// this is the NER label of the token
					lemma = token.get(LemmaAnnotation.class);
				}

			}

			String[] splits = null;

			// Convert POS in terms of Sentiword

			HashMap<String, String> mappingPOS = new HashMap<String, String>();
			mappingPOS.put("NN", "n");
			mappingPOS.put("NNS", "n");
			mappingPOS.put("NNP", "n");
			mappingPOS.put("NNPS", "n");
			mappingPOS.put("VB", "v");
			mappingPOS.put("VBD", "v");
			mappingPOS.put("VBG", "v");
			mappingPOS.put("VBN", "v");
			mappingPOS.put("VBP", "v");
			mappingPOS.put("VBZ", "v");
			mappingPOS.put("JJ", "a");
			mappingPOS.put("JJR", "a");
			mappingPOS.put("JJS", "a");
			mappingPOS.put("RB", "r");
			mappingPOS.put("RBR", "r");
			mappingPOS.put("RBS", "r");

			if (mappingPOS.get(pos) != null) {
				pos = mappingPOS.get(pos);
			}

			Double score = 0.0;

			// insert when found
			if ((dictionary.get(word[i] + "#" + pos)) != null) {

				score = dictionary.get(word[i] + "#" + pos);
				System.out.println("Inside dictionary" + word[i] + "#" + pos);
				System.out.println("score" + score);
				wordPoints.put(word[i], score);

			} else {

				// if not present in list

				for (String key : dictionary.keySet()) {
					splits = key.split("#");
					if (pos.equalsIgnoreCase(splits[1])) {

						score = findRelatedness(word[i], key);
						if (notFound.get(word[i]) != null) {
							wordRelatedness = notFound.get(word[i]);
						}
						wordRelatedness.put(key, score);
						notFound.put(word[i], wordRelatedness);
					}
				}
			}

		}

	}

	public static Double findRelatedness(String word, String dictionaryWord) {

		ILexicalDatabase lexDb = new NictWordNet();
		RelatednessCalculator rc = new Lesk(lexDb);
		String[] splits = dictionaryWord.split("#");

		Double score = rc.calcRelatednessOfWords(word, splits[0]);

		return score;

	}

	public static void clearNotFound() {

		HashMap<String, Double> wordScoreSWN = new HashMap<String, Double>();
		for (String key : notFound.keySet()) {

			// checking if the word has been found
			// If found then no need of using relatedness
			if (wordPoints.get(key) != null) {
				System.out.println("skipped");
				continue;
			}
			wordScoreSWN = notFound.get(key);

			// sort the score values

			List<Map.Entry<String, Double>> maxScore = new LinkedList<Map.Entry<String, Double>>(
					wordScoreSWN.entrySet());

			Collections.sort(maxScore,
					new Comparator<Map.Entry<String, Double>>() {

						public int compare(Map.Entry<String, Double> map1,
								Map.Entry<String, Double> map2) {

							return (map2.getValue()).compareTo(map1.getValue());
						}

					});

			// the word with the highest score is most close
			// get the first word score after sorting if greater than 0.6

			String word = "";
			Double score = 0D;

			for (Iterator<Map.Entry<String, Double>> iterator = maxScore
					.iterator(); iterator.hasNext();) {

				Map.Entry<String, Double> entry = iterator.next();

				System.out.println("Word and Score - " + entry.getKey() + " "
						+ entry.getValue());

				word = entry.getKey();
				score = entry.getValue();
				// just require the first value
				break;

			}
			if (score > 0.6) {

				wordPoints.put(key, dictionary.get(word) * score);

			} else {
				// word wasn't in either file and not close enough to any word
				wordPoints.put(key, 0.0);
			}

		}

	}

}
