package com.projecta.consumerex;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerCooperativeStickyAssignor {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCooperativeStickyAssignor.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka consumer");

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "172.20.122.172:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "groupTwoEx");
        properties.setProperty("auto.offset.reset","earliest");

        // Generally kafka stops the world when a consumer is added or removed, instead by using
        // CooperativeStickyAssignor - Only affected partition re-balance happens
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // If a consumer rejoins, there is no guarantee that the consumer will get same partition
        // unless static assignment is enabled and consumer joins within the timeout
        // properties.setProperty("group.instance.id", "test")

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList("topicEx"));

        final Thread mainThread = Thread.currentThread();

        // Adds a hook that runs when JVM shutdowns
        // Runs on a seperate thread from main thread
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");

            // Special method to interrupt consumer poll call
            consumer.wakeup();

            try{
                // As shutdown hook thread runs seperately, it has to join after main thread such that main thread handles wakeup call and closes consumer
                mainThread.join();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }));

        try{
            //poll data
            while(true)
            {
                log.info("polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records){
                    log.info("Key: " + record.key() + "Key:" + record.value());
                }
            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close();
        }

    }

}
