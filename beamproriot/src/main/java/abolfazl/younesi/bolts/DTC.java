package abolfazl.younesi.bolts;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.CSVLoader;
import weka.gui.visualize.PlotData2D;
import weka.gui.visualize.ThresholdVisualizePanel;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Random;

public class DTC {
    public static void dtcClassify(String csvFilePath, String outputFolder) {
        try {
            System.out.println("Loading CSV data...");
            CSVLoader loader = new CSVLoader();
            loader.setSource(new File(csvFilePath));
            Instances data = loader.getDataSet();

            System.out.println("Setting class attribute...");
            data.setClassIndex(data.attribute(" payment_type").index());

            System.out.println("Initializing decision tree classifier...");
            J48 tree = new J48();
            tree.setUnpruned(false); // Unpruned tree

            System.out.println("Training and evaluating the model...");
            Evaluation eval = trainAndEvaluateModel(tree, data);

            System.out.println("Evaluation Metrics:");
            System.out.println("------------------------------");
            System.out.println("Mean Absolute Error: " + eval.meanAbsoluteError());
            System.out.println("Root Mean Squared Error: " + eval.rootMeanSquaredError());
            System.out.println("Relative Absolute Error: " + eval.relativeAbsoluteError());
            System.out.println("Root Relative Squared Error: " + eval.rootRelativeSquaredError());

            System.out.println("Saving the trained model...");
            saveModel(tree);

            System.out.println("Generating visualizations...");
            generateVisualizations(eval, data, outputFolder);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Evaluation trainAndEvaluateModel(J48 tree, Instances data) throws Exception {
        int nFolds = 5; // Number of folds for cross-validation
        Evaluation eval = new Evaluation(data);
        eval.crossValidateModel(tree, data, nFolds, new Random(1));
        return eval;
    }

    private static void saveModel(J48 tree) throws Exception {
        try {
            String modelFilePath = "F:\\utf-8-FOIL2013\\FOIL2013\\output" + File.separator + "decision_tree.model";
            SerializationHelper.write(modelFilePath, tree);
            System.out.println("Model saved successfully: " + modelFilePath);
        } catch (Exception e) {
            System.err.println("Error saving model:");
            e.printStackTrace();
        }
    }

    private static void generateVisualizations(Evaluation eval, Instances data, String outputFolder) throws Exception {
        // Generate accuracy and loss chart
        generateAccuracyLossChart(eval, outputFolder);

        // Generate ROC curve visualization
        generateROCCurveVisualization(eval, data, outputFolder);
    }

    private static void generateAccuracyLossChart(Evaluation eval, String outputFolder) throws Exception {
        // Create a chart for accuracy and loss
        XYSeries accuracySeries = new XYSeries("Accuracy");
        XYSeries lossSeries = new XYSeries("Loss");

        for (int i = 0; i < eval.numInstances(); i++) {
            accuracySeries.add(i, eval.pctCorrect());
            lossSeries.add(i, eval.rootMeanSquaredError());
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(accuracySeries);
        dataset.addSeries(lossSeries);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Accuracy and Loss",
                "Instances",
                "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        // Save the chart as PNG file
        String outputFileName = outputFolder + File.separator + "accuracy_and_loss_chart.png";
        ChartUtils.saveChartAsPNG(new File(outputFileName), chart, 800, 600);
        System.out.println("Chart saved as PNG file: " + outputFileName);
    }

    private static void generateROCCurveVisualization(Evaluation eval, Instances data, String outputFolder) throws Exception {
        // Create a chart for ROC curve
        ThresholdVisualizePanel vmc = new ThresholdVisualizePanel();
        vmc.setROCString("(Area under ROC) - Class 0: " + eval.areaUnderROC(0) + ", Class 1: " + eval.areaUnderROC(1));
        vmc.setName(data.relationName());
        PlotData2D tempPlot = new PlotData2D(data);
        tempPlot.setPlotName(data.relationName());
        tempPlot.addInstanceNumberAttribute();

        // Specify which points are connected
        boolean[] cp = new boolean[data.numInstances()];
        for (int n = 1; n < cp.length; n++)
            cp[n] = true;
        tempPlot.setConnectPoints(cp);

        // Add plot to the visualization panel
        vmc.addPlot(tempPlot);

        // Display the ROC curve
        String plotName = vmc.getName();
        JFrame jf = new JFrame("Decision Tree Visualizer: " + plotName);
        jf.setSize(800, 600);
        jf.getContentPane().setLayout(new BorderLayout());
        jf.getContentPane().add(vmc, BorderLayout.CENTER);
        jf.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent e) {
                jf.dispose();
            }
        });
        jf.setVisible(true);

        // Save the ROC curve chart as PNG file
        String outputFileName = outputFolder + File.separator + "decision_tree_visualization.png";
        saveChartAsPNG(outputFileName, vmc);
        System.out.println("ROC curve chart saved as PNG file: " + outputFileName);
    }

    private static void saveChartAsPNG(String outputFileName, Component component) {
        try {
            BufferedImage image = new BufferedImage(component.getWidth(), component.getHeight(), BufferedImage.TYPE_INT_ARGB);
            component.paint(image.getGraphics());
            javax.imageio.ImageIO.write(image, "PNG", new File(outputFileName));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        dtcClassify("F:\\utf-8-FOIL2013\\FOIL2013\\output\\chunk_1.csv","F:\\utf-8-FOIL2013\\FOIL2013\\output");
    }

}