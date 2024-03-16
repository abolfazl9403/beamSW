package abolfazl.younesi.beamutil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CSVSplitter {
    public static int splitCSV(String inputFile, String outputFolder, int numberOfChunks) throws IOException {
        if (!Files.exists(Paths.get(outputFolder))) {
            Files.createDirectories(Paths.get(outputFolder));
            System.out.println("Dataset files already exists!");
        }

        // Count the total number of rows in the CSV file
        int totalRows = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            while (br.readLine() != null) {
                totalRows++;
            }
        }

        // Calculate the approximate chunk size
        int chunkSize = totalRows / numberOfChunks;
        int remainder = totalRows % numberOfChunks;

        // Read and split the CSV file into chunks
        int chunkNumber = 1;
        int filesWritten = 0; // Counter for files written
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            for (int i = 0; i < numberOfChunks; i++) {
                String outputFileName = outputFolder + "/Java_chunk_" + chunkNumber + ".csv";

                // Check if the output file already exists, if not then write to it
                if (!Files.exists(Paths.get(outputFileName))) {
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName))) {
                        for (int j = 0; j < chunkSize; j++) {
                            String line = br.readLine();
                            if (line != null) {
                                writer.write(line);
                                writer.newLine();
                            }
                        }
                        chunkNumber++;
                        filesWritten++;
                        System.out.println("Output file generated: " + outputFileName); // Print output file generation
                    }
                    // If there is a remainder, distribute the remaining rows among the first few chunks
                    if (remainder > 0) {
                        String line = br.readLine();
                        if (line != null) {
                            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName, true))) {
                                writer.write(line);
                                writer.newLine();
                            }
                        }
                        remainder--;
                    }
                } else {
                    // Output file already exists, move to the next chunk
                    chunkNumber++;
                }
            }
        }
        return filesWritten;
    }
}


//write a code that is training the decision tree  on the below CSV  data, save the model, and draw a chart of the accuracy and loss