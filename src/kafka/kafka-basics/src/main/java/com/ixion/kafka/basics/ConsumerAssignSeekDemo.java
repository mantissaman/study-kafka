package com.ixion.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeekDemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String topic ="first_topic";

        //create configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // earliest/latest/none

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to reply data or fetch specific message
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom =15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead =5;
        boolean keepOnreading = true;
        int numberOfMessagesReadSofar =0;

        //poll for new data
        while(keepOnreading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records){
                numberOfMessagesReadSofar += 1;
                logger.info("Key: "+ record.key() + ", Value: "+ record.value());
                logger.info("Partition: "+ record.partition() + ", Offset: "+ record.offset());
                if(numberOfMessagesReadSofar>=numberOfMessagesToRead){
                    keepOnreading = false;
                    break;
                }
            }
        }

        logger.info("Application exited");


    }
}
