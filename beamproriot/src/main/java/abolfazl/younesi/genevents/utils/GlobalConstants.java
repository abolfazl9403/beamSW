package abolfazl.younesi.genevents.utils;

import abolfazl.younesi.App;

public class GlobalConstants {
    public static String mqttBroker = "tcp://broker.emqx.io:1883";
    public static String clientIdPublisher = "Publisher";
    public static String clientIdSubscriber = "Subscriber";
    public static String publisherTopic= "topic/test_pub";
    public static int pubQos = 1;
    public static final long INTERVAL = 1000; // Monitoring interval in milliseconds
    public static final long WINDOW_SIZE = 10; // Size of the sliding window in seconds
    public static final int MAX_RECONNECT_ATTEMPTS = 5;
    public static int numberOfChunks = 20;
    public static int windowBlockSize = 10;
    public static String dataFilePath = App.dtc+"\\chunk_2.csv";

}
