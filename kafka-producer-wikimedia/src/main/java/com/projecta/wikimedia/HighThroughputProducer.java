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

public class HighThroughputProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //High throughput configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        //cautious in allocating this, because memory is allocated per partition
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));



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
