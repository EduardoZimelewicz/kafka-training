package com.github.eduardozimelewicz.udemy.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class UserPurchasesProducer {
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

        //Create a purchase producer
        Producer<String, String> purchaseProducer = new KafkaProducer<>(config);
        
        ProducerRecord<String, String> user1Purchase =
                new ProducerRecord<>("user-purchases-input", "alice", "microwave");
        purchaseProducer.send(user1Purchase);
        Thread.sleep(1000);

        ProducerRecord<String, String> purchase1 =
                new ProducerRecord<>("purchase-data-input", "alice", "100");
        purchaseProducer.send(purchase1);
        Thread.sleep(1000);

        ProducerRecord<String, String> user2Purchase =
                new ProducerRecord<>("user-purchases-input", "bob", "television");
        purchaseProducer.send(user2Purchase);
        Thread.sleep(1000);

        ProducerRecord<String, String> purchase2 =
                new ProducerRecord<>("purchase-data-input", "bob", "200");
        purchaseProducer.send(purchase2);
        Thread.sleep(1000);

        ProducerRecord<String, String> user3Purchase =
                new ProducerRecord<>("user-purchases-input", "ed", "laptop");
        purchaseProducer.send(user3Purchase);
        Thread.sleep(1000);
    }
}
