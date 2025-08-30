package com.projecta.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SafeProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Till 2.8 default is 1 which means only leader ack is received, post 2.8 default is -1 or all
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString((Integer.MAX_VALUE)));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        URI uri = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");
        EventSource eventSource = new EventSource.Builder(eventHandler, uri).headers(Headers.of("User-Agent", "MyApp/1.0 (your_email@example.com)")).build();


        //Start the producer on another thread
        eventSource.start();

        // As prodicer is async, we block the program for 10 minutes
        TimeUnit.MINUTES.sleep(10);
    }
}
