package abolfazl.younesi.bolts;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Queue;
// Class for calculating block window average
public class BlockWindowAverage {
    private final Queue<TaxiData> blockData;  // Queue to store taxi data for the current block
    private final int blockSize;
    private double blockSum;  // Sum of total amounts in the current block
    private int fileIndex; // To keep track of processed files
    private final StringBuilder averageData; // Accumulator for average values

    // Constructor to initialize block window average calculator
    public BlockWindowAverage(int blockSize) {
        this.blockSize = blockSize;
        this.blockData = new ArrayDeque<>(blockSize);
        this.blockSum = 0;
        this.fileIndex = 1; // Initialize file index
        this.averageData = new StringBuilder();
    }

    // Method to add taxi data to the current block
    public void addData(TaxiData data) {
        blockData.add(data);
        blockSum += data.getTotalAmount();
        if (blockData.size() > blockSize) {
            TaxiData removedData = blockData.poll();
            blockSum -= removedData.getTotalAmount();
        }
    }

    // Method to calculate the average total amount for the current block
    public double getAverage() {
        return blockSum / blockData.size();
    }

    public void saveAverageToFile(String directory) {
        try (FileWriter writer = new FileWriter(directory + "/average_values.txt")) {
            writer.write(averageData.toString());
        } catch (IOException e) {
            System.err.println("Error writing average values to file: " + e.getMessage());
        }
    }
    // Method to save processed data for the current block to individual files
    public void saveProcessedData(String directory, int blockSize) {
        // Create the output directory if it doesn't exist
        File outputDir = new File(directory);
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                System.err.println("Failed to create directory: " + directory);
                return;
            }
        }

        // Write block data to individual files
        try (PrintWriter writer = new PrintWriter(new File(directory, "BWA_chunk_" + blockSize + "_" + fileIndex + ".csv"))) {
            for (TaxiData data : blockData) {
                writer.println(dataToString(data));
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error saving processed data: " + e.getMessage());
        }

        // Accumulate average data
        double average = getAverage();
        averageData.append(average).append("\n");

        fileIndex++; // Increment file index after processing each file
    }

    // Method to write accumulated average data to a single file
    public void writeAveragesToFile(String directory) {
        // Create the output directory if it doesn't exist
        File outputDir = new File(directory);
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                System.err.println("Failed to create directory: " + directory);
                return;
            }
        }

        // Write accumulated average data to a single file
        try (PrintWriter writer = new PrintWriter(new File(directory, "BWA_Averages.csv"))) {
            writer.println(averageData);
        } catch (FileNotFoundException e) {
            System.err.println("Error writing average data to file: " + e.getMessage());
        }
    }

    // Method to convert TaxiData object to string
    private String dataToString(TaxiData data) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return data.getMedallion() + "," +
                data.getHackLicense() + "," +
                data.getVendorId() + "," +
                sdf.format(data.getPickupDatetime()) + "," +
                data.getPaymentType() + "," +
                data.getFareAmount() + "," +
                data.getSurcharge() + "," +
                data.getMtaTax() + "," +
                data.getTipAmount() + "," +
                data.getTollsAmount() + "," +
                data.getTotalAmount();
    }
}
