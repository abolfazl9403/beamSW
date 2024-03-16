package abolfazl.younesi.spout;

import abolfazl.younesi.App;  
import weka.classifiers.trees.J48;  
import weka.classifiers.Evaluation;  
import weka.core.SerializationHelper;  
import weka.core.Instances;  
import weka.core.converters.CSVLoader;  

import java.io.FileWriter;  
import java.io.PrintWriter;  
import java.io.File;  
import java.io.IOException;  

public class DTTest {
    public static J48 loadModel(String modelFilePath) throws IOException, ClassNotFoundException { // Method for loading the saved model

        try {
            System.out.println("Loading model from file: " + modelFilePath);
            J48 model = (J48) SerializationHelper.read(modelFilePath);
            if (model == null) {
                System.err.println("Error: Loaded model is null.");
                throw new RuntimeException("Loaded model is null.");
            }
            System.out.println("Model loaded successfully." + model);
            return model;
        } catch (Exception e) {
            System.err.println("Error loading model: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static Instances loadTestData(String csvFilePath) throws IOException { // Method for loading test data from a CSV file

        try {
            System.out.println("Loading test data from file: " + csvFilePath);
            CSVLoader loader = new CSVLoader();

            loader.setSource(new File(csvFilePath));
            Instances data = loader.getDataSet();

            // Set class attribute
            data.setClassIndex(data.attribute(" payment_type").index());

            System.out.println("Test data loaded successfully.");
            return data;
        } catch (IOException e) {
            System.err.println("Error loading test data: " + e.getMessage());
            throw e;
        }
    }

    public static void classifyTestData(J48 model, Instances testData) { // Method for classifying test data using the loaded model
        // Make predictions on test instances
        try {
            System.out.println("Classifying test data...");
            for (int i = 0; i < testData.size(); i++) {
//                System.out.println("data: " + testData.get(i));
                double predictedClass = model.classifyInstance(testData.get(i)); // Classifying test instance
//                System.out.println("Instance " + (i + 1) + ": Predicted class = " + testData.classAttribute().value((int) predictedClass)); 
            }
            System.out.println("Test data classified successfully."); 
        } catch (Exception e) {
            System.err.println("Error classifying test data: " + e.getMessage()); // Printing error message
        }
    }

    public static void evaluateModel(J48 model, Instances testData, String outputFilePath) { // Method for evaluating the model's performance
        try {
            System.out.println("Evaluating model on test data...");
            Evaluation eval = new Evaluation(testData);
            eval.evaluateModel(model, testData);

            FileWriter fileWriter = new FileWriter(outputFilePath);
            PrintWriter printWriter = new PrintWriter(fileWriter);

            // Writing evaluation metrics to the output file
            printWriter.println(eval.toSummaryString());
            printWriter.println(eval.toMatrixString("Confusion Matrix"));
            printWriter.println("Accuracy: " + eval.pctCorrect() + "%");
            printWriter.println("Mean Absolute Error: " + eval.meanAbsoluteError());
            printWriter.println("Root Mean Squared Error: " + eval.rootMeanSquaredError());
            printWriter.println("Relative Absolute Error: " + eval.relativeAbsoluteError());
            printWriter.println("root Relative Squared Error: " + eval.rootRelativeSquaredError());

            printWriter.close();

            System.out.println("Evaluation metrics saved to: " + outputFilePath);

            // Printing evaluation metrics
            System.out.println(eval.toSummaryString());
            System.out.println(eval.toMatrixString("Confusion Matrix"));
            System.out.println("Accuracy: " + eval.pctCorrect() + "%");
            System.out.println("Mean Absolute Error: " + eval.meanAbsoluteError());
            System.out.println("Root rootMeanSquaredError: " + eval.rootMeanSquaredError());
            System.out.println("Mean Squared Error: " + eval.rootRelativeSquaredError());
            System.out.println("Relative Absolute Error: " + eval.relativeAbsoluteError());
            // You can also access other metrics such as precision, recall, F-measure, etc. using eval.precision(), eval.recall(), eval.fMeasure() etc.
        } catch (Exception e) {
            System.err.println("Error evaluating model: " + e.getMessage());
            e.printStackTrace();
        }
    }

//public static void main(String[] args) {
//        try {
//            // Load the saved model
//            String modelFilePath = "E:\\beampro\\beamproriot\\DTC.model";
//            System.out.println("Loading the saved model...");
//            J48 model = loadModel(modelFilePath);
//
//            if (model == null) {
//                System.err.println("Error: Loaded model is null.");
//                return; // Exit the program
//            }
//
//            // Load test data
//            String fileName= "chunk_3.csv";
//            String csvFilePath = App.dtc+"\\"+fileName; // Replace with your test data file path
//            System.out.println("Loading test data...");
//            Instances testData = loadTestData(csvFilePath);
//
//            if (testData.isEmpty()) {
//                System.err.println("Error: Test data is null or empty.");
//                return; // Exit the program
//            }
//
//            // Classify test instances
//            System.out.println("Classifying test instances...");
//            classifyTestData(model, testData);
//
//            // Evaluate model and save metrics to a text file
//            String outputFilePath = "evaluation_metrics_"+fileName+".txt";
//            evaluateModel(model, testData, outputFilePath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
