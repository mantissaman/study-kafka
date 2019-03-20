package com.ixion.kafka.twitter;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {
        String host = "127.0.0.1";
        int port = 9200;
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"));

        RestHighLevelClient client = new RestHighLevelClient( builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers="127.0.0.1:9092";
        String groupId ="kafka-elastic";

        //create configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // earliest/latest/none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        //create consumer
        KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        try {


            KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                Integer recordCount =  records.count();
                logger.info("Received " + recordCount+ " recrods.");

                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record: records){
                    //insert into elastic search

                    //idempotence? generic Id
                    // String id = record.topic() +"_"+record.partition()+"_"+record.offset();
                    try {
                        //twitter specific id
                        String id = extractIdFromTweet(record.value());

                        String jsonString = record.value();
                        IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                                .source(jsonString, XContentType.JSON);

                        bulkRequest.add(indexRequest); //add to bulk request
                    }catch(Exception e){
                        logger.warn("skipping bad data " + record.value());
                    }


                }
                if(recordCount >0) {
                    BulkResponse bulkItemresponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                    logger.info("Committing the offsets ...");
                    consumer.commitSync();
                    logger.info("Offsets have been committed.");
                    try {
                        Thread.sleep(1000); //intorduce a small delay
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            // client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson){
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("id_str")
                    .getAsString();


    }

}
