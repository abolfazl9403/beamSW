package abolfazl.younesi.spout;

import abolfazl.younesi.genevents.utils.GlobalConstants;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import weka.classifiers.trees.J48;
import weka.classifiers.Evaluation;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.SerializationHelper;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

import java.io.*;

public class DTCMqtt {
    public static J48 loadModel(String modelFilePath) throws IOException, ClassNotFoundException {
        // Load the saved model
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
            e.printStackTrace(); // Print stack trace for detailed error information
            throw new RuntimeException(e);
        }
    }

    public static void subscribeToMQTT(String topic, String broker, String clientId, J48 model) {
        try {
            MemoryPersistence persistence = new MemoryPersistence();
            IMqttClient mqttClient = new MqttClient(broker, clientId, persistence);

            System.out.println(clientId);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: " + broker);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            mqttClient.setCallback(new MyMqttCallback(model, mqttClient)); // Pass both model and mqttClient instances
            mqttClient.subscribe(topic, 1);

            System.out.println("Subscribed to topic: " + topic);
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

//     Define your MqttCallback implementation
static class MyMqttCallback implements MqttCallback {
    private J48 model;
    private IMqttClient mqttClient;

    public MyMqttCallback(J48 model, IMqttClient mqttClient) {
        this.model = model;
        this.mqttClient=mqttClient;
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("Connection lost: " + cause.getMessage());

        // Clear any previously received messages or state
        // Add code here to clear the state of your application

        // Attempt to reconnect
        boolean reconnected = false;
        int retryCount = 0;
        int maxRetries = 5; // Adjust the maximum number of retries as needed

        while (!reconnected && retryCount < maxRetries) {
            try {
                System.out.println("Attempting to reconnect... (Attempt " + (retryCount + 1) + ")");
                mqttClient.reconnect();
                System.out.println("Reconnected to the MQTT broker");
                reconnected = true;
            } catch (MqttException e) {
                System.err.println("Failed to reconnect: " + e.getMessage());
                retryCount++;

                // Wait for a short period before retrying
                try {
                    Thread.sleep(5000); // Wait for 5 seconds (adjust as needed)
                } catch (InterruptedException ex) {
                    // Handle the interrupted exception if needed
                }
            }
        }

        if (!reconnected) {
            System.err.println("Maximum reconnection attempts reached. Unable to reconnect to the MQTT broker.");
            // You can add additional error handling or exit logic here
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        String messageContent = new String(mqttMessage.getPayload());
        Instance instance = parseMessageToInstance(messageContent);

        double predictedClass = model.classifyInstance(instance);

        System.out.println("Received Message: " + messageContent);
        System.out.println("Predicted class: " + instance.classAttribute().value((int) predictedClass));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("Delivery complete");
    }

    private Instance parseMessageToInstance(String messageContent) throws IOException {
        try {
            // Split the message content by commas to get individual attribute values
            String[] attributeValues = messageContent.split(",");

            // Check if the number of attribute values matches the expected number of attributes
            if (attributeValues.length != 11) {
                throw new IOException("Invalid number of attributes in the message");
            }

            // Create a new instance with the appropriate number of attributes
            Instance instance = new DenseInstance(11);

            // Set the attribute values for the instance
            instance.setValue(0, attributeValues[0]); // medallion
            instance.setValue(1, attributeValues[1]); // hack_license
            instance.setValue(2, attributeValues[2]); // vendor_id
            instance.setValue(3, attributeValues[3]); // pickup_datetime

            // Set the payment_type attribute value
            String paymentType = attributeValues[4];
            Instances dataSet = instance.dataset();
            instance.setValue(4, dataSet.attribute("payment_type").addStringValue(paymentType));

            instance.setValue(5, Double.parseDouble(attributeValues[5])); // fare_amount
            instance.setValue(6, Double.parseDouble(attributeValues[6])); // surcharge
            instance.setValue(7, Double.parseDouble(attributeValues[7])); // mta_tax
            instance.setValue(8, Double.parseDouble(attributeValues[8])); // tip_amount
            instance.setValue(9, Double.parseDouble(attributeValues[9])); // tolls_amount
            instance.setValue(10, Double.parseDouble(attributeValues[10])); // total_amount
            System.out.println("Created Instance: " + instance.toString());
            // Set the class index
            instance.dataset().setClassIndex(4); // Assuming payment_type is the class attribute

            return instance;
        } catch (Exception e) {
            throw new IOException("Error parsing message to instance: " + e.getMessage(), e);
        }
    }


}

    public static Instances loadTestData(String csvFilePath) throws IOException {
        // Load test data from CSV file
        try {
            System.out.println("Loading test data from file: " + csvFilePath);
            CSVLoader loader = new CSVLoader();
            loader.setSource(new File(csvFilePath));
            Instances data = loader.getDataSet();

            // Set class attribute
            data.setClassIndex(data.attribute(" payment_type").index()); // Replace with your class attribute name

            System.out.println("Test data loaded successfully.");
            return data;
        } catch (IOException e) {
            System.err.println("Error loading test data: " + e.getMessage());
            throw e;
        }
    }

    public static void classifyTestData(J48 model, Instances testData) {
        // Make predictions on test instances
        try {
            System.out.println("Classifying test data...");
            for (int i = 0; i < testData.size(); i++) {
                System.out.println(testData.get(i));
                double predictedClass = model.classifyInstance(testData.get(i));
                System.out.println("Instance " + (i + 1) + ": Predicted class = " + testData.classAttribute().value((int) predictedClass));
            }
            System.out.println("Test data classified successfully.");
        } catch (Exception e) {
            System.err.println("Error classifying test data: " + e.getMessage());
        }
    }

    public static void evaluateModel(J48 model, Instances testData, String outputFilePath) {
        try {
            System.out.println("Evaluating model on test data...");
            Evaluation eval = new Evaluation(testData);
            eval.evaluateModel(model, testData);
            FileWriter fileWriter = new FileWriter(outputFilePath);
            PrintWriter printWriter = new PrintWriter(fileWriter);

            printWriter.println(eval.toSummaryString());
            printWriter.println(eval.toMatrixString("Confusion Matrix"));
            printWriter.println("Accuracy: " + eval.pctCorrect() + "%");
            printWriter.println("Mean Absolute Error: " + eval.meanAbsoluteError());
            printWriter.println("Root Mean Squared Error: " + eval.rootMeanSquaredError());
            printWriter.println("Relative Absolute Error: " + eval.relativeAbsoluteError());
            printWriter.println("root Relative Squared Error: " + eval.rootRelativeSquaredError());

            printWriter.close();

            System.out.println("Evaluation metrics saved to: " + outputFilePath);

            System.out.println(eval.toSummaryString()); // Prints the summary of evaluation
            System.out.println(eval.toMatrixString("Confusion Matrix")); // Prints the confusion matrix
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

    public static void main(String[] args) {
        try {
            // Load the saved model
            String modelFilePath = "E:\\beampro\\beamproriot\\DTC.model";

            System.out.println("Loading the saved model...");
            J48 model = loadModel(modelFilePath);

            if (model == null) {
                System.err.println("Error: Loaded model is null.");
                return; // Exit the program
            }

            // MQTT subscription configuration
            String topic = "topic/test_pub"; // MQTT topic to subscribe to
            String broker = GlobalConstants.mqttBroker; // MQTT broker address
            String clientId = MqttClient.generateClientId();


            // Subscribe to MQTT topic
            subscribeToMQTT(topic, broker, clientId ,model);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
