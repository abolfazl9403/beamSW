## Project Title and Description:
**Title:** Beam Processing Pipeline for Taxi Dataset

This repository contains a Java application that performs various data processing tasks on taxi trip data, including CSV splitting, ARFF conversion, decision tree classification, block window averaging, and MQTT messaging.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Weka library (included in the project dependencies)
- MQTT broker (e.g., Mosquitto)

## Getting Started

1. Clone the repository:

   ```
   git clone https://github.com/abolfazlyounesi/beamSW.git
   ```

2. Navigate to the project directory:

   ```
   cd beamproiot
   ```

3. Build the project using Maven:

   ```
   mvn clean package
   ```

4. Update the `GlobalConstants` class with the appropriate file paths and configuration values for your environment.


## Usage Instructions:
1. Ensure all dependencies and configurations are set up correctly.
2. Run the main class `App.java` with appropriate arguments.
3. Provide input CSV file path and output folder path.
4. Ensure configurations for DTC, BWA, and MQTT are correctly set.
5. Execute the pipeline and monitor the logs for progress.
6. Upon completion, check the specified output directories for results.


## Credits and Acknowledgments:
- Apache Beam for providing the framework for data processing.
- Weka library for machine learning functionalities.
- MQTT for facilitating data communication.
- Contributors to third-party libraries used in the project.
- 
## Features

- **CSV Splitting**: The application can split a large CSV file into multiple smaller chunks, which can be useful for parallel processing or handling large datasets.
- **CSV to ARFF Conversion**: The CSV data is converted to ARFF format, which is required by the Weka machine learning library for training and classification.
- **Decision Tree Classification (DTC)**: The application performs decision tree classification using the J48 algorithm from the Weka library. It trains a model and classifies the data based on the provided input.
- **Block Window Average (BWA)**: The application calculates the average of a specific attribute (e.g., total amount) over a fixed window size using a sliding window approach.
- **Model Evaluation**: The application can load a pre-trained decision tree model, classify test instances, and evaluate the model's performance by calculating various evaluation metrics.
- **MQTT Publisher**: The application publishes the CSV data to an MQTT broker, which can be useful for streaming data or integrating with other systems.
- **MQTT Subscriber**: The application subscribes to an MQTT topic and receives messages from the broker.
- **Apache Beam Pipeline**: The application utilizes the Apache Beam library for parallel data processing, including reading input data from a CSV file, calculating block window averages, and writing the results to an output file.

## Configuration

The application relies on several configuration values defined in the `GlobalConstants` class. Make sure to update these values according to your environment and requirements:

- `dataFilePath`: Path to the input CSV file containing taxi trip data.
- `numberOfChunks`: Number of chunks to split the CSV file into.
- `windowBlockSize`: Size of the sliding window for block window averaging.
- `mqttBroker`: Address of the MQTT broker.
- `clientIdPublisher`: Client ID for the MQTT publisher.
- `publisherTopic`: Topic to publish messages to.
- `pubQos`: Quality of Service (QoS) level for MQTT publishing.

## Dependencies

The project relies on the following third-party libraries:

- Apache Beam
- Apache Flink
- Weka
- Eclipse Paho MQTT Client

These dependencies are managed by Maven and included in the project's `pom.xml` file.
