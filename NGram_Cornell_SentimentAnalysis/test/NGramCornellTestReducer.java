package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NGramCornellTestReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text> {

	private Text comment = new Text();
	private IntWritable score = new IntWritable(); 
	private Text type = new Text("neutral");
	private static HashMap<String, Double> wordPoints = new HashMap<String, Double>();

	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		int sum = 0;
		//HashMap<String, Integer> commentNGrams = new HashMap<String, Integer>();
		HashMap<String, Integer> posModelNGrams = new HashMap<String, Integer>();
		HashMap<String, Integer> negModelNGrams = new HashMap<String, Integer>();

		// Reading positive and negative ngrams dictionary formed
		String posPath = "/home/saurabh/NgramData/posNGram.txt";
		posModelNGrams = readModelNGrams(posPath);
		
		String negPath = "/home/saurabh/NgramData/negNGram.txt";
		negModelNGrams = readModelNGrams(negPath);
		
		int score = 0;
		String keys = "";
		
		while (values.hasNext()) {	
			
            keys = values.next().toString();
			if(posModelNGrams.get(keys) != null){
				score += posModelNGrams.get(keys);
			}else if(negModelNGrams.get(keys) != null){
				score -= negModelNGrams.get(keys);
			}

		}			
		
		if(score > 0 ){
			type.set("positive");
		}else if (score < 0){
			type.set("negative");
		}
		
		comment.set("Comment " + "----->>> ");
		output.collect(comment, type);
	}

	public static HashMap<String, Integer> readModelNGrams(String path) {

		HashMap<String, Integer> nGrams = new HashMap<String, Integer>();
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		String line = "";
		String[] splits = null;
		StringTokenizer tokenizer = null;

		try {
			fileReader = new FileReader(path);
			bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {

				tokenizer = new StringTokenizer(line,"\t");
				
			    //Reading the train data ngrams
				while (tokenizer.hasMoreTokens()){
			             nGrams.put(tokenizer.nextToken(), Integer.parseInt(tokenizer.nextToken()));
			      }


			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(bufferedReader != null){
				try {
					bufferedReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(fileReader != null){
				try {
					fileReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}

		return nGrams;
	}


}
