package abolfazl.younesi.beamutil;

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CSVToARFF {
    public static void convertCSVsToARFFs(String csvDirectory, String arffDirectory) {
        File csvFolder = new File(csvDirectory);
        File[] csvFiles = csvFolder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null) {
            System.err.println("No CSV files found in the directory: " + csvDirectory);
            return;
        }

        for (File csvFile : csvFiles) {
            String arffFileName = arffDirectory + "/" + csvFile.getName().replace(".csv", ".arff");
            File arffFile = new File(arffFileName);

            if (arffFile.exists()) {
                System.out.println("ARFF file already exists for: " + csvFile.getName());
                continue; // Skip conversion
            }

            try {
                System.out.println("Converting: " + csvFile.getName() + " to ARFF...");
                convertCSVtoARFF(csvFile.getAbsolutePath(), arffFileName);
                System.out.println("Converted: " + csvFile.getName() + " -> " + arffFileName);
            } catch (IOException e) {
                System.err.println("Error converting " + csvFile.getName() + " to ARFF: " + e.getMessage());
            }
        }
    }

    private static void convertCSVtoARFF(String csvFile, String arffFile) throws IOException {
        // Open the CSV file for reading
        System.out.println("Opening CSV file: " + csvFile);
        BufferedReader csvReader = new BufferedReader(new FileReader(csvFile));
        // Open the ARFF file for writing
        System.out.println("Creating ARFF file: " + arffFile);
        BufferedWriter arffWriter = new BufferedWriter(new FileWriter(arffFile));

        // Write ARFF header
        arffWriter.write("@relation data\n\n");

        // Read the attribute names from the first row
        String[] attributes = csvReader.readLine().split(",");
        // Write attribute declarations
        for (String attribute : attributes) {
            // Remove leading/trailing spaces and single quotes
            attribute = attribute.trim().replaceAll("^'+|'+$", "");
            if (attribute.equalsIgnoreCase("vendor_id")) {
                arffWriter.write("@attribute " + attribute + " {VTS,CMT}\n");
            } else if (attribute.equalsIgnoreCase("pickup_datetime")) {
                arffWriter.write("@attribute " + attribute + " date 'yyyy-MM-dd HH:mm:ss'\n");
            } else if (attribute.equalsIgnoreCase("payment_type")) {
                arffWriter.write("@attribute " + attribute + " {CSH,CRD,NOC,DIS,UNK}\n");
            } else {
                arffWriter.write("@attribute " + attribute + " numeric\n");
            }
        }
        arffWriter.write("\n@data\n");

        // Read each line from CSV and write to ARFF
        String row;
        while ((row = csvReader.readLine()) != null) {
            arffWriter.write(row + "\n");
        }

        // Close readers and writers
        System.out.println("Closing CSV and ARFF files...");
        csvReader.close();
        arffWriter.close();
    }
}
