package com.ixion.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThreademo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerThreademo.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String groupId ="my_app_group";
        String topic ="first_topic";

        //create configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // earliest/latest/none

        //create consumer


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to topics
        consumer.subscribe(Collections.singleton(topic));

        //poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records){
                logger.info("Key: "+ record.key() + ", Value: "+ record.value());
                logger.info("Partition: "+ record.partition() + ", Offset: "+ record.offset());
            }
        }


    }

    public class ConsumerThread implements Runnable{

        CountDownLatch latch;

        public ConsumerThread(CountDownLatch latch){
            this.latch = latch;
        }

        @Override
        public void run() {

        }

        public void shutdown(){

        }
    }
}
