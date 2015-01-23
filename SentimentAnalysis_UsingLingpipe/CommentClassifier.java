package reviewComment;

import java.io.File;
import java.io.IOException;

import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;

public class CommentClassifier {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String comment = "the movie was somewhat good but very lengthy";

		String dataPath = "/home/saurabh/CornellData";
		String[] categories = null;
		String type = "";
		try {
			LMClassifier classifier = (LMClassifier)AbstractExternalizable.readObject(new File(dataPath +"/Classifier"));
			categories = classifier.categories();
			
			type = (classifier.classify(comment)).bestCategory();
			
			System.out.println("This comment is "+"!!"+type+ "!!");
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
