package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NGramKaggleTestReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub

		String val = "", rating = "";
		int count0 = 0, count1 = 0, count2 = 0, count3 = 0, count4 = 0;

		HashMap<String, String> trainData = new HashMap<String, String>();

		String trainPath = "/home/saurabh/KaggleData/trainData.txt";
		trainData = readModelNGrams(trainPath);
		int max = 0;

		// can be changed to just have one final value
		while (values.hasNext()) {

			val = values.next().toString();
			System.out.println("value: "+val);
			if (trainData.get(val) != null) {
				System.out.println("in");
				
				rating = trainData.get(val);
				System.out.println("rating:"+rating);

				if (rating.equals("0")) {
					count0++;
				} else if (rating.equals("1")) {
					count1++;
				} else if (rating.equals("2")) {
					count2++;
				} else if (rating.equals("3")) {
					count3++;
				} else if (rating.equals("4")) {
					count4++;
				}				
				
				//System.out.println("count:0: "+count0+" 1: "+count1+" 2: "+count2+" 3: "+count3+" 4: "+count4);
			}

		}
		
		max = getMax(count0, count1, count2, count3, count4);
			System.out.println("Phrase Id: "+key+" max: "+ max);

		// rating -1 will show no match
		output.collect(key, new IntWritable(max));
	}
	

	public static HashMap<String, String> readModelNGrams(String path) {

		HashMap<String, String> nGrams = new HashMap<String, String>();
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		String line = "";
		StringTokenizer tokenizer = null;

		try {
			fileReader = new FileReader(path);
			bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {

				tokenizer = new StringTokenizer(line,"\t");
				
			    //Reading the train data ngrams
				while (tokenizer.hasMoreTokens()){
			             nGrams.put(tokenizer.nextToken(), tokenizer.nextToken());
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

		return nGrams;
	}
	
	
	public static int getMax(int a, int b, int c, int d, int e) {

		ArrayList<Integer> array = new ArrayList<Integer>();

		array.add(a);
		array.add(b);
		array.add(c);
		array.add(d);
		array.add(e);
		
		int rating =-1;
		
		if(a== 0 && b==0 && c==0 && d==0 && e==0)
			return rating;

		int max = Collections.max(array);
		
		
		if (max == a) {
			rating = 0;
		} else if (max == b) {
			rating = 1;
		} else if (max == c) {
			rating = 2;
		} else if (max == d) {
			rating = 3;
		} else if (max == e) {
			rating = 4;
		}

		return rating;
	}
}
