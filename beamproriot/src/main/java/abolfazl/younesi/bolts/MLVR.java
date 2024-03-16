package abolfazl.younesi.bolts;

import org.jfree.chart.ChartUtils;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.evaluation.NumericPrediction;
import weka.classifiers.evaluation.Prediction;
import weka.classifiers.evaluation.Evaluation;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MLVR {
    public static void main(String[] args) {
        try {
            // Load CSV data
            System.out.println("Loading CSV data...");
            File csvFile = new File("F:\\utf-8-FOIL2013\\FOIL2013\\output\\chunk_2.csv");

            if (!csvFile.exists()) {
                throw new IOException("CSV file not found.");
            }

            CSVLoader loader = new CSVLoader();
            loader.setSource(csvFile);
            Instances data = loader.getDataSet();

            // Set the class attribute index
            data.setClassIndex(data.attribute(" tip_amount").index());

            // Apply NominalToBinary filter
//            System.out.println("Converting nominal attributes to binary...");
//            NominalToBinary filter = new NominalToBinary();
//            filter.setInputFormat(data);
//            Instances filteredData = Filter.useFilter(data, filter);

            // Train Linear Regression model
            System.out.println("Training Linear Regression model...");

            // Define batch size
            int batchSize = 10;

            // Train Linear Regression model in batches
            LinearRegression model = new LinearRegression();
            for (int i = 0; i < data.numInstances(); i += batchSize) {
                System.out.println("Training Linear Regression model..."+i);
                Instances batch = new Instances(data, i, Math.min(batchSize, data.numInstances() - i));
                model.buildClassifier(batch);
            }

            // Save the model
            System.out.println("Saving the model...");
            weka.core.SerializationHelper.write("linear_regression_model.model", model);

            // Evaluate the model
            System.out.println("Evaluating the model...");
            Evaluation evaluation = new Evaluation(data);
            evaluation.evaluateModel(model, data);

            // Print evaluation metrics
            System.out.println("Evaluation Metrics:");
            System.out.println("------------------------------");
            System.out.println("Mean Absolute Error: " + evaluation.meanAbsoluteError());
            System.out.println("Root Mean Squared Error: " + evaluation.rootMeanSquaredError());
            System.out.println("Relative Absolute Error: " + evaluation.relativeAbsoluteError());
            System.out.println("Root Relative Squared Error: " + evaluation.rootRelativeSquaredError());
            System.out.println("Correlation Coefficient: " + evaluation.correlationCoefficient());
            System.out.println("Coefficient of Determination: " + evaluation.correlationCoefficient());
            System.out.println(evaluation.toSummaryString());

            // Get predictions
            System.out.println("Generating predictions...");
            List<Prediction> predictions = new ArrayList<>();
            for (int i = 0; i < data.numInstances(); i++) {
                double actual = data.instance(i).classValue();
                double predicted = model.classifyInstance(data.instance(i));
                predictions.add(new NumericPrediction(actual, predicted));
            }

            // Plot accuracy and loss
            System.out.println("Plotting accuracy and loss...");

            XYSeries series = new XYSeries("Predicted vs. Actual");
            for (int i = 0; i < predictions.size(); i++) {
                double actual = predictions.get(i).actual();
                double predicted = predictions.get(i).predicted();
                series.add(i, predicted);
            }

            XYSeriesCollection dataset = new XYSeriesCollection(series);
            JFreeChart chart = ChartFactory.createXYLineChart(
                    "Linear Regression Model Evaluation",
                    "Instance Number",
                    "Predicted",
                    dataset
            );

            // Save the chart as an image
            System.out.println("Saving the plot as an image...");
            File chartFile = new File("accuracy_loss_chart.png");
            ChartUtils.saveChartAsPNG(chartFile,
                    chart,
                    800,
                    600);

            System.out.println("Process completed successfully!");
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
