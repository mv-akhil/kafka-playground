package com.projecta.consumerex;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerEx {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEx.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka consumer");

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "172.20.122.172:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "groupTwoEx");
        properties.setProperty("auto.offset.reset","none");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList("topicEx"));

        //poll data
        while(true)
        {
            log.info("polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records){
                log.info("Key: " + record.key() + "Key:" + record.value());
            }
        }
    }

}
