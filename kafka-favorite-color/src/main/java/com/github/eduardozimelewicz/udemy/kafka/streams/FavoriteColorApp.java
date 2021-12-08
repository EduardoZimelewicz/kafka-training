package com.github.eduardozimelewicz.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class FavoriteColorApp {
    public static void main (String [] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> favoriteColors = builder.stream("favorite-color-input");

        KStream<String, String> favoriteColorsIntermediary = favoriteColors
                .filter((key, value) -> value.matches("\\w+,\\w+"))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((key, value) -> value.equals("red")
                        || value.equals("blue")
                        || value.equals("green"));

        favoriteColorsIntermediary.to("user-colors");

        KTable<String, String> userColorsTable = builder.table("user-colors");

        KTable<String, Long> favoriteColorOverall = userColorsTable
                .groupBy((key, value) -> new KeyValue<>(value, value))
                .count("Counts");

        favoriteColorOverall.to(Serdes.String(), Serdes.Long(), "favorite-color-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
