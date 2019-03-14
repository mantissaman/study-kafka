package com.ixion.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers="127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers",bootstrapServers);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer",StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer <K, V>
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++) {
            // Create producer record

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world " + i);

            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime record is successfully sent or exception is thrown
                    if (e == null) {
                        //recordMetadata.
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
            producer.flush();

        }

        producer.close();

    }
}
