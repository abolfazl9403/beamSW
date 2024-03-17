package abolfazl.younesi.spout;

import abolfazl.younesi.App;
import abolfazl.younesi.genevents.utils.GlobalConstants;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MQTTPub {

    public static void main(String[] args) {
        String broker = GlobalConstants.mqttBroker;
        String clientId = GlobalConstants.clientIdPublisher;
        String pubTopic = GlobalConstants.publisherTopic;
        int pubQos = 1;
        String msg = GlobalConstants.dataFilePath;

        try {
            System.out.println("Connecting to MQTT broker...");
            MqttClient client = new MqttClient(broker, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            client.connect(options);

            if (client.isConnected()) {
                System.out.println("Connected to MQTT broker.");
                Thread publishThread = new Thread(() -> {
                    try {
                        // Publish CSV file line by line with delay
                        try (BufferedReader br = new BufferedReader(new FileReader(msg))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                MqttMessage message = new MqttMessage(line.getBytes());
                                message.setQos(pubQos);
                                client.publish(pubTopic, message);

                                // Introduce delay between publishing each message (adjust delay time as needed)
                                System.out.println("Published message: " + line);
                                Thread.sleep(1); // Delay for .1 second (100 milliseconds)
                            }
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                });
                publishThread.start();
            }

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
