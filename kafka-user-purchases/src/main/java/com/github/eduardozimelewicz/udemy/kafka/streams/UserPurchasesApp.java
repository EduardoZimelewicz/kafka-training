package com.github.eduardozimelewicz.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class UserPurchasesApp {
    public static void main (String [] args){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-purchases-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> userPurchases = builder.stream("user-purchases-input");

        GlobalKTable<String, String> userData = builder.globalTable("purchase-data-input");

        KStream<String, String> userInnerJoin = userPurchases.join(userData,
                (leftKey, leftValue) -> leftKey,
                (leftValue, rightValue) -> leftValue + ", " + rightValue
        );

        userInnerJoin.to("user-inner-join-output");

        KStream<String, String> userLeftJoin =  userPurchases.leftJoin(userData,
                (leftKey, leftValue) -> leftKey,
                (leftValue, rightValue) -> leftValue + ", " + rightValue
        );

        userLeftJoin.to("user-left-join-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
