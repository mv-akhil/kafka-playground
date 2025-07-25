package com.projecta.ex;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerEx {

    private static final Logger log = LoggerFactory.getLogger(ProducerEx.class.getSimpleName());
    public static void main(String[] args) {
        log.info("HELLO");
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.20.122.172:9092");

        //// In kafka data is exchanged in bytes, you have to tell kafka on how to convert your data into bytes.
        //// On consumer side as well, consumer should follow the same to get expected data
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        //// In the producer we are defining data types of the key and value, also passing in properties for the producer to know how to connect.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topicEx", "Hello world");

        // send data
        //// Sending data is async process
        producer.send(producerRecord);

        //// flush is sync process, tells the producer to send all the data, block until done
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
