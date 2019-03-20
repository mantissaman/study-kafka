package com.ixion.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilter {
    final Logger logger = LoggerFactory.getLogger(StreamsFilter.class.getName());
    public static void main(String[] args) {

        String bootstrapServers="127.0.0.1:9092";
        String groupId ="kafka-elastic";

        //create configs
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputTopic=builder.stream("twitter_tweets");
        KStream<String, String> filterStream=inputTopic.filter((k,jsonTweet) ->extractUserFollowersFromTweet(jsonTweet) > 10000);
        filterStream.to("important_tweets");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);


        kafkaStreams.start();

    }
    private static JsonParser jsonParser = new JsonParser();
    private static int extractUserFollowersFromTweet(String tweetJson){
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch(Exception e){
            return 0;
        }


    }

}
