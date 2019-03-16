package com.ixion.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThreadDemo {
    public static void main(String[] args) {
        new ConsumerThreadDemo().run();
    }
    private ConsumerThreadDemo(){

    }

    private void run(){
        final Logger logger = LoggerFactory.getLogger(ConsumerThreadDemo.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String groupId ="my_app_group";
        String topic ="first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRuunable(bootstrapServers,
                groupId,
                topic,
                latch);

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Caught shutdown hook");
                    ((ConsumerRuunable) myConsumerRunnable).shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        logger.error("Application shutdown is interrupted",e);
                    } finally{
                        logger.info("Application is shutting down");
                    }
                    logger.info("Application is shut down");
                }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is interrupted",e);
        } finally{
            logger.info("Application is closing");
        }
    }


    public class ConsumerRuunable implements Runnable{
        private final Logger logger = LoggerFactory.getLogger(ConsumerRuunable.class.getName());

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRuunable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch){

            this.latch = latch;

            //create configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // earliest/latest/none

            this.consumer = new KafkaConsumer<String, String>(properties);
            this.consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }
            catch(WakeupException e){
                logger.info("Received Shutdown signal.");
            }
            finally{
                this.consumer.close();
                //tell the main code that we're done with the consumer
                this.latch.countDown();
            }
        }

        public void shutdown(){
            //the wakeup interrupts consumer.poll
            //it will throw an exception WakeUpexception
            this.consumer.wakeup();
        }
    }
}
