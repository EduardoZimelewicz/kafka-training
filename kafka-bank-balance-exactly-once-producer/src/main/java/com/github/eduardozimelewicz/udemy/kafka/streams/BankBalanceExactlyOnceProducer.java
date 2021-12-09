package com.github.eduardozimelewicz.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class BankBalanceExactlyOnceProducer {
    public static void main (String [] args) throws InterruptedException{
        Properties config = new Properties();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        config.put(ProducerConfig.RETRIES_CONFIG, "3");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        //Create a producer
        Producer<String, String> bankAccountProducer = new KafkaProducer<>(config);

        while(true) {
            //John
            ProducerRecord<String, String> johnRecord =
                    new ProducerRecord<>("bank-balance-input","John",
                            createTransactionRecord("John").toString());
            bankAccountProducer.send(johnRecord);
            Thread.sleep(100);

            //Alice
            ProducerRecord<String, String> aliceRecord =
                    new ProducerRecord<>("bank-balance-input","Alice",
                            createTransactionRecord("Alice").toString());
            bankAccountProducer.send(aliceRecord);
            Thread.sleep(100);

            //Bob
            ProducerRecord<String, String> bobRecord =
                    new ProducerRecord<>("bank-balance-input","Bob",
                            createTransactionRecord("Bob").toString());
            bankAccountProducer.send(bobRecord);
            Thread.sleep(100);

            //Eduardo
            ProducerRecord<String, String> eduardoRecord =
                    new ProducerRecord<>("bank-balance-input","Eduardo",
                            createTransactionRecord("Eduardo").toString());
            bankAccountProducer.send(eduardoRecord);
            Thread.sleep(100);

            //Luke
            ProducerRecord<String, String> lukeRecord =
                    new ProducerRecord<>("bank-balance-input","Luke",
                            createTransactionRecord("Luke").toString());
            bankAccountProducer.send(lukeRecord);
            Thread.sleep(100);

            //Phil
            ProducerRecord<String, String> philRecord =
                    new ProducerRecord<>("bank-balance-input","Phil",
                            createTransactionRecord("Phil").toString());
            bankAccountProducer.send(philRecord);
            Thread.sleep(100);
        }
    }

    private static JsonNode createTransactionRecord(String name){
        ObjectMapper transactionMapper = new ObjectMapper();
        ObjectNode transaction = transactionMapper.createObjectNode();

        transaction.put("Name", name);
        transaction.put("Amount", generateAmount());
        transaction.put("Time", Instant.now().toString());

        return transaction;
    }

    private static int generateAmount() {
        Random random = new Random();
        int upperbound = 50;
        int int_random = random.nextInt(upperbound);
        return int_random;
    }
}
