package abolfazl.younesi;

import abolfazl.younesi.beamutil.*;

import abolfazl.younesi.bolts.BWABeam;
import abolfazl.younesi.bolts.BlockWindowAverage;
import abolfazl.younesi.bolts.DTC;
import abolfazl.younesi.bolts.TaxiData;
import abolfazl.younesi.genevents.utils.GlobalConstants;
import abolfazl.younesi.spout.DTTest;

import abolfazl.younesi.spout.MQTTBeamPub;
import abolfazl.younesi.spout.MQTTSub;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.trees.J48;
import weka.core.Instances;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;



public class App {
    public static final Logger LOG = LoggerFactory.getLogger(App.class);
    public static String dtc = "F:\\utf-8-FOIL2013\\FOIL2013\\output";

    private static PCollection<String> readInputData(Pipeline p, String inputFilePath) {
        return p.apply("ReadData",
                TextIO.read().from(inputFilePath));
    }

    private static PCollection<String> readCSVLines(Pipeline p, String csvInputFile) {
        return p.apply("ReadCSVDataLine",
                TextIO.read().from(csvInputFile));
    }

    private static PCollection<List<String>> splitIntoChunks(PCollection<String> lines) {
        return lines.apply("SplitIntoChunks",
                MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via((SerializableFunction<String, List<String>>) line -> {
                            assert line != null;
                            return Arrays.asList(line.split(","));
                        }));
    }

    private static void writeChunks(PCollection<List<String>> chunks, String outputFolder, int numberOfChunks) {
        chunks.apply("WriteChunks", FileIO.<List<String>>write()
                .via(Contextful.fn((List<String> chunk) -> {
                    assert chunk != null;
                    return chunk.stream().collect(Collectors.joining("\n"));
                }), TextIO.sink())
                .to(outputFolder)
                .withPrefix("chunk_")
                .withSuffix(".csv")
                .withNumShards(numberOfChunks));
    }

    // DoFn to invoke dtcClassify method
    static class InvokeDTC extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Void> out) {
            try {
                // Call dtcclassify() method from DTC class
                DTC.dtcClassify(GlobalConstants.dataFilePath,dtc);
            } catch (Exception e) {
                // Log error and continue processing
                LOG.warn("Error invoking dtcClassify(): {}", e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
        System.out.println("Starting the pipeline...");
        // Read input data
//        String inputFilePath = "path/to/input/data";
//        PCollection<String> inputData = readInputData(p, inputFilePath);

        //-------------------------------------------

        //--- Read input data
        System.out.println("Reading input data...");
        String csvInputFile = "F:\\utf-8-FOIL2013\\FOIL2013\\trip_fare_1\\trip_fare_1.csv"; //---- Replace with your CSV file path
        String chunkOutputFolder = "F:\\utf-8-FOIL2013\\FOIL2013\\output"; //----- Replace with your output folder path
        String arffOutputFile = "F:\\utf-8-FOIL2013\\FOIL2013\\arffoutput";

//        PCollection<String> csvInputData = readCSVLines(p, csvInputFile);

        //---- Split CSV lines into chunks
        System.out.println("Splitting CSV lines into chunks...");
//        PCollection<List<String>> chunks = splitIntoChunks(csvInputData);

        //--- Write chunks to output
        System.out.println("Writing chunks to output...");
//        writeChunks(chunks, outputFolder, numberOfChunks);

        DTC.dtcClassify(GlobalConstants.dataFilePath,dtc);

        // BEAM BWA
        PCollection<String> taxiData_ = p.apply(TextIO.read().from(GlobalConstants.dataFilePath));

        PCollection<KV<Integer, Double>> averages = taxiData_
                .apply(ParDo.of(new BWABeam.CalculateBlockAverage()))
                .apply(Window.<KV<Integer, Double>>into(FixedWindows.of(Duration.standardMinutes(1))));

        PCollection<String> averagesAsStrings = averages.apply(ParDo.of(new BWABeam.KVToStringFn()));

        averagesAsStrings
                .apply(TextIO.write()
                        .to(App.dtc + "\\BWA")
                        .withHeader(BWABeam.HEADER)
                        .withNumShards(4));

        try {
            int numFilesWritten = CSVSplitter.splitCSV(csvInputFile, chunkOutputFolder, GlobalConstants.numberOfChunks);
            System.out.println("Total number of files written: " + numFilesWritten);

            CSVToARFF.convertCSVsToARFFs(chunkOutputFolder, arffOutputFile);
            System.out.println("Conversion completed successfully.");

            p.apply("ReadInputData", TextIO.read().from(GlobalConstants.dataFilePath))
                    .apply("InvokeDTC", MapElements.into(TypeDescriptors.strings()).via((String line) -> {
                        try {
                            DTC.dtcClassify(GlobalConstants.dataFilePath,dtc);
                        } catch (Exception e) {
                            LOG.warn("Error invoking dtcClassify(): {}", e.getMessage());
                        }
                        return ""; // or any other value as per your requirement
                    }));

            // Block window average

            BlockWindowAverage blockWindowAverage = new BlockWindowAverage(GlobalConstants.windowBlockSize);

            String outputDirectory = App.dtc + "\\BWA"; // Provide the path to the output directory
            String line;
            String cvsSplitBy = ",";

            // Check if the input file exists
            File inputFile = new File(GlobalConstants.dataFilePath);
            if (!inputFile.exists()) {
                System.err.println("Input file does not exist: " + GlobalConstants.dataFilePath);
                return;
            }

            try (BufferedReader br = new BufferedReader(new FileReader(GlobalConstants.dataFilePath))) {
                while ((line = br.readLine()) != null) {
                    String[] data = line.split(cvsSplitBy);
                    // Skipping header row
                    if (!data[0].equals("medallion")) {
                        try {
                            TaxiData taxiData = new TaxiData(data);
                            blockWindowAverage.addData(taxiData);
                            // Save processed data to new file for each block
                            blockWindowAverage.saveProcessedData(outputDirectory, GlobalConstants.windowBlockSize);
                            System.out.println("Average total amount for block: " + blockWindowAverage.getAverage());
                        } catch (ParseException e) {
                            System.err.println("Error parsing data: " + e.getMessage());
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading file: " + e.getMessage());
            }

            blockWindowAverage.saveAverageToFile(outputDirectory);
            // Write accumulated average data to file
//        blockWindowAverage.writeAveragesToFile(outputDirectory);



            // DTC test
//          Load the saved model
            String modelFilePath = "E:\\beampro\\beamproriot\\DTC.model";
            System.out.println("Loading the saved model...");
            J48 model = DTTest.loadModel(modelFilePath);

            if (model == null) {
                System.err.println("Error: Loaded model is null.");
                return; // Exit the program
            }

            // Load test data
            String fileName= "chunk_3.csv";
            String csvFilePath = App.dtc+"\\"+fileName; // Replace with your test data file path
            System.out.println("Loading test data...");
            Instances testData = DTTest.loadTestData(csvFilePath);

            if (testData.isEmpty()) {
                System.err.println("Error: Test data is null or empty.");
                return; // Exit the program
            }

            // Parallelize classification and evaluation
            ExecutorService executor = Executors.newFixedThreadPool(2);

            executor.submit(() -> {
                // Classify test instances
                System.out.println("Classifying test instances...");
                DTTest.classifyTestData(model, testData);
            });

            executor.submit(() -> {
                // Evaluate model and save metrics to a text file
                String outputFilePath = "evaluation_metrics_" + fileName + ".txt";
                DTTest.evaluateModel(model, testData, outputFilePath);
            });

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);


            // mqtt publisher
            LOG.info("Reading CSV file: {}", GlobalConstants.dataFilePath);
            PCollection<String> csvLines = p.apply("ReadCSV", TextIO.read().from(GlobalConstants.dataFilePath));

            LOG.info("Publishing to MQTT broker: {}", GlobalConstants.mqttBroker);
            csvLines.apply("PublishToMQTT", ParDo.of(new MQTTBeamPub.PublishToMQTTFn(GlobalConstants.mqttBroker, GlobalConstants.clientIdPublisher, GlobalConstants.publisherTopic, GlobalConstants.pubQos)));

            // mqtt subscriber
//            MQTTSub.mqttSub();

        } catch (IOException e) {
            System.err.println("Error occurred while splitting CSV file: " + e.getMessage());
            e.printStackTrace();
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        //--- Run the pipeline
        System.out.println("Running the pipeline...");
        p.run().waitUntilFinish();

        System.out.println("The Program has been executed successfully.");

    }

}
