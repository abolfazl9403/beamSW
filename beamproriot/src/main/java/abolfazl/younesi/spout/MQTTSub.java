package abolfazl.younesi.spout;

import abolfazl.younesi.genevents.utils.GlobalConstants;
import org.eclipse.paho.client.mqttv3.*;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MQTTSub {
    private static long messageCount = 0;
    private static long startTime;
    public static long individualLatency;
    private static final Queue<Long> messageTimestamps = new ArrayDeque<>(); // Sliding window for message timestamps

    public static void mqttSub() {
        String broker = GlobalConstants.mqttBroker;
        String clientId = GlobalConstants.clientIdSubscriber;
        String subTopic = GlobalConstants.publisherTopic;
        int subQos = 1;

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        try {
            System.out.println("Connecting to MQTT broker...");
            MqttClient client = new MqttClient(broker, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            client.connect(options);

            if (client.isConnected()) {
                System.out.println("Connected to MQTT broker.");
                // Subscribe in a separate thread
                executorService.execute(() -> subscribe(client, subTopic, subQos));
                // Start monitoring in another separate thread
                executorService.execute(MQTTSub::startMonitoring);
            }

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static void subscribe(MqttClient client, String subTopic, int subQos) {
        try {
            client.setCallback(new MqttCallback() {
                public void messageArrived(String topic, MqttMessage message) {
                    long currentTime = System.currentTimeMillis();
                    messageCount++;
                    messageTimestamps.offer(currentTime); // Add message timestamp to sliding window

                    // Calculate latency
                    long latency = currentTime - startTime;
                    individualLatency = latency / messageCount;

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
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    // Method to start monitoring
    private static void startMonitoring() {
        while (true) {
            try {
                Thread.sleep(GlobalConstants.INTERVAL);
                updateMetrics();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // Method to update metrics using sliding window approach
    private static void updateMetrics() {
        long currentTime = System.currentTimeMillis();

        // Remove old message timestamps from the sliding window
        while (!messageTimestamps.isEmpty() && messageTimestamps.peek() < currentTime - (GlobalConstants.WINDOW_SIZE * 1000)) {
            messageTimestamps.poll();
            messageCount--; // Decrease message count for removed messages
        }

        // Calculate throughput within the sliding window
        double elapsedTime = (currentTime - startTime) / 1000.0; // Convert to seconds
        double throughput = messageCount / elapsedTime;
        System.out.println("Throughput: " + throughput + " messages/second");

        // Calculate CPU utilization
        double cpuUsage = getCpuUsage();
        System.out.println("CPU Utilization: " + cpuUsage + "%");

        // Calculate memory utilization
        double memoryUsage = getMemoryUsage();
        System.out.println("Memory Utilization: " + memoryUsage + "%");

        saveMetricsToCSV(individualLatency, throughput, cpuUsage, memoryUsage);
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

    // Method to save metrics to CSV file
    private static void saveMetricsToCSV(long currentTime, double throughput, double cpuUsage, double memoryUsage) {
        String csvFile = "metrics.csv";
        try (FileWriter writer = new FileWriter(csvFile, true)) {
            writer.append(String.valueOf(currentTime)).append(",");
            writer.append(String.valueOf(throughput)).append(",");
            writer.append(String.valueOf(cpuUsage)).append(",");
            writer.append(String.valueOf(memoryUsage)).append("\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
