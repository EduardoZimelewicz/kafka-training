package com.github.eduardozimelewicz.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.util.Properties;

public class BankBalanceExactlyOnce {
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> bankAmounts =
                builder.stream("bank-balance-input");

        KTable<String,String> bankBalance = bankAmounts
                .groupByKey()
                .aggregate(
                        () -> newBalance().toString(),
                        (aggKey, newValue, aggValue) -> computeBalance(newValue, aggValue).toString(),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregated-bank-balance")
                                .withValueSerde(Serdes.String())
                );

        bankBalance.toStream().to("bank-balance-output");

        return builder.build();
    }

    public static void main (String [] args){
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        BankBalanceExactlyOnce bankBalanceExactlyOnce = new BankBalanceExactlyOnce();

        KafkaStreams streams = new KafkaStreams(bankBalanceExactlyOnce.createTopology(), config);

        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(){
        ObjectMapper balanceMapper = new ObjectMapper();
        ObjectNode balance = balanceMapper.createObjectNode();

        balance.put("Name", "");
        balance.put("Amount", 0);
        balance.put("Time", "");

        return balance;
    }

    private static JsonNode computeBalance(String newValue, String aggValue){
        ObjectMapper balanceMapper = new ObjectMapper();

        JsonNode newValueNode = null;
        JsonNode aggValueNode = null;

        try {
            newValueNode = balanceMapper.readTree(newValue);
            aggValueNode = balanceMapper.readTree(aggValue);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int balance = aggValueNode.get("Amount").asInt() + newValueNode.get("Amount").asInt();

        ObjectNode accountBalance = balanceMapper.createObjectNode();
        accountBalance.put("Amount", balance);
        accountBalance.put("Time", newValueNode.get("Time").asText());

        return accountBalance;
    }

}