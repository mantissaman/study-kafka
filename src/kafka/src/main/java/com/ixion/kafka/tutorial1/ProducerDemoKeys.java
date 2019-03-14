package com.ixion.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            // Create producer record

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(key, topic, value);

            logger.info("Key: " + key);
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
            }).get(); // block to send it synchronous

            producer.flush();

        }

        producer.close();

    }
}
