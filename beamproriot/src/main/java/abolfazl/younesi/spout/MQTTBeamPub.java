package abolfazl.younesi.spout;

import abolfazl.younesi.App;
import abolfazl.younesi.genevents.utils.GlobalConstants;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTBeamPub {
    private static final Logger logger = LoggerFactory.getLogger(MQTTBeamPub.class);

//    public static void main(String[] args) {
//        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
//
//
//        logger.info("Reading CSV file: {}", GlobalConstants.dataFilePath);
//        PCollection<String> csvLines = pipeline.apply("ReadCSV", TextIO.read().from(GlobalConstants.dataFilePath));
//
//        logger.info("Publishing to MQTT broker: {}", GlobalConstants.mqttBroker);
//        csvLines.apply("PublishToMQTT", ParDo.of(new PublishToMQTTFn(GlobalConstants.mqttBroker, GlobalConstants.clientIdPublisher, GlobalConstants.publisherTopic, GlobalConstants.pubQos)));
//
//        logger.info("Starting pipeline execution");
//        pipeline.run().waitUntilFinish();
//        logger.info("Pipeline execution completed");
//    }

    public static class PublishToMQTTFn extends DoFn<String, Void> {
        private final String broker;
        private final String clientId;
        private final String pubTopic;
        private final int pubQos;
        private MqttClient client;
        private static final Logger logger = LoggerFactory.getLogger(PublishToMQTTFn.class);


        public PublishToMQTTFn(String broker, String clientId, String pubTopic, int pubQos) {
            this.broker = broker;
            this.clientId = clientId;
            this.pubTopic = pubTopic;
            this.pubQos = pubQos;
        }

        @Setup
        public void setup() throws MqttException, InterruptedException {
            MqttConnectOptions options = new MqttConnectOptions();
            int attempts = 0;
            while (attempts < GlobalConstants.MAX_RECONNECT_ATTEMPTS) {
                try {
                    client = new MqttClient(broker, clientId);
                    client.connect(options);
                    logger.info("Connected to MQTT broker");
                    break; // Break the loop if connection successful
                } catch (MqttException e) {
                    logger.error("Failed to connect to MQTT broker. Retrying...");
                    attempts++;
                    Thread.sleep(10); // Wait for 1 second before retrying
                }
            }
            if (attempts == GlobalConstants.MAX_RECONNECT_ATTEMPTS) {
                throw new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED);
            }
        }

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Void> out) throws MqttException {
            System.out.println("Processing element: " + line);
            if (client.isConnected()) {
                MqttMessage message = new MqttMessage(line.getBytes());
                message.setQos(pubQos);
                client.publish(pubTopic, message);
                System.out.println("Published message to topic: " + pubTopic);
            } else {
                System.err.println("MQTT client is not connected. Cannot publish message.");
            }
        }


        @Teardown
        public void teardown() throws MqttException {
            if (client != null && client.isConnected()) {
                client.disconnect();
                logger.info("Disconnected from MQTT broker");
            }
        }
    }
}
