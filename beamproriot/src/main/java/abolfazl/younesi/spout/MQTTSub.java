package abolfazl.younesi.spout;

import abolfazl.younesi.genevents.utils.GlobalConstants;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;

public class MQTTSub {

    // Variables for monitoring
    private static long startTime;
    private static long messageCount = 0;
    private static final long INTERVAL = 2000; // Monitoring interval in milliseconds

    public static void main(String[] args) {
        String broker = GlobalConstants.mqttBroker;
        String clientId = "Subscriber";
        String subTopic = "topic/test_pub";
        int subQos = 1;

        try {
            System.out.println("Connecting to MQTT broker...");
            MqttClient client = new MqttClient(broker, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            client.connect(options);

            if (client.isConnected()) {
                System.out.println("Connected to MQTT broker.");
                // Subscribe in a separate thread
                Thread subscribeThread = new Thread(() -> {
                    try {
                        client.setCallback(new MqttCallback() {
                            public void messageArrived(String topic, MqttMessage message) throws Exception {
                                long currentTime = System.currentTimeMillis();
                                messageCount++;

                                // Calculate latency
                                long latency = currentTime - startTime;
                                long individualLatency = latency / messageCount;

                                System.out.println("Message arrived:");
                                System.out.println("  Topic: " + topic);
                                System.out.println("  QoS: " + message.getQos());
                                System.out.println("  Content: " + new String(message.getPayload()));
                                System.out.println("  Latency: " + individualLatency + " ms");
                                System.out.println("  Total Latency: " + latency + " ms");
                            }

                            public void connectionLost(Throwable cause) {
                                System.out.println("Connection lost: " + cause.getMessage());
                            }

                            public void deliveryComplete(IMqttDeliveryToken token) {
                                System.out.println("Delivery complete: " + token.isComplete());
                            }
                        });

                        client.subscribe(subTopic, subQos);
                        System.out.println("Subscribed to topic: " + subTopic);

                        // Start monitoring
                        startTime = System.currentTimeMillis();
                        startMonitoring();
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                });
                subscribeThread.start();
            }

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    // Method to start monitoring
    private static void startMonitoring() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(INTERVAL);
                    // Calculate throughput
                    long currentTime = System.currentTimeMillis();
                    double elapsedTime = (currentTime - startTime) / 1000.0; // Convert to seconds
                    double throughput = messageCount / elapsedTime;
                    System.out.println("Throughput: " + throughput + " messages/second");

                    // Calculate CPU utilization
                    double cpuUsage = getCpuUsage();
                    System.out.println("CPU Utilization: " + cpuUsage + "%");

                    // Calculate memory utilization
                    double memoryUsage = getMemoryUsage();
                    System.out.println("Memory Utilization: " + memoryUsage + "%");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    // Method to get CPU utilization
    private static double getCpuUsage() {
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        double cpuUsage = osBean.getSystemLoadAverage();

        if (cpuUsage == -1.0) {
            cpuUsage = 0.0;
        }
        return cpuUsage;
    }

    // Method to get memory utilization
    private static double getMemoryUsage() {
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memBean.getHeapMemoryUsage();
        return (double) heapMemoryUsage.getUsed() / heapMemoryUsage.getMax() * 100.0;
    }
}
