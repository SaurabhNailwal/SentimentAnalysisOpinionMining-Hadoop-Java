package reviewComment;

import java.io.File;
import java.io.IOException;

import com.aliasi.classify.Classification;
import com.aliasi.classify.Classified;
import com.aliasi.classify.DynamicLMClassifier;
import com.aliasi.classify.LMClassifier;
import com.aliasi.corpus.ObjectHandler;
import com.aliasi.util.AbstractExternalizable;
import com.aliasi.util.Compilable;
import com.aliasi.util.Files;

public class ClassifierTraining {

	/**
	 * @param args
	 */
	public static void main(String[] args){
		// TODO Auto-generated method stub
		
		String dataPath = "/home/saurabh/CornellData";		
		String cat = "";
		
		int numOfComments = 0;
		int nGram = 8;
		File trainingData = new File(dataPath);
		String[] categories = trainingData.list();
		
		LMClassifier classifier = DynamicLMClassifier.createNGramProcess(categories, nGram);
        
		for(int i=0;i<categories.length;i++){
			cat = categories[i]; //individual folder inside given path
			
			Classification classfication = new Classification(cat);
			File file = new File(trainingData,cat);
			File[] files = file.listFiles();
			File trainFile =null;
			String comment = "";
			for(int j=0;j<files.length;j++){
				trainFile = files[j];
				try {
					comment = Files.readFromFile(trainFile, "ISO-8859-1");
					numOfComments++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Classified classified = new Classified(comment, classfication);
				
				((ObjectHandler)classifier).handle(classified);
			}
			System.out.println("Folder "+cat+" comments :"+numOfComments);
			numOfComments = 0;
		}
		
		try {
			AbstractExternalizable.compileTo((Compilable)classifier, new File(dataPath +"/Classifier"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
