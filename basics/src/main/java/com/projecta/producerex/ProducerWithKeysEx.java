package com.projecta.producerex;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeysEx {

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


        // Round robin - Distributes messages evenly across partitions when no key is provided
        // sticky - Sends messages without a key to one partition for a short time to optimize batching then switches partition
        // If key is present both round robin and sticky are bypassed when partition is choosen based on key where batching happens at partition level

        for(int j=0; j<=1; j++){
            for(int i = 0; i <=20; i++){

                String key = "id_" + i;

                // create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topicEx", key, "Hello world_" + i);

                // send data
                //// Sending data is async process
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            log.info("Key:" + key + ", Partition:" + recordMetadata.partition() + ", Offset:" + recordMetadata.offset());
                        } else {
                            log.error("Error occurred", e);
                        }
                    }
                });
            }
        }

        //// flush is sync process, tells the producer to send all the data, block until done
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
