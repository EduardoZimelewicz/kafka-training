package com.github.eduardozimelewicz.udemy.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.eduardozimelewicz.udemy.kafka.streams.BankBalanceExactlyOnce;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

import static org.junit.Assert.assertNull;

public class BankExactlyOnceTest {
    private TopologyTestDriver testDriver;

    private final StringSerializer stringSerializer = new StringSerializer();

    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    @Before
    public void setUpTopologyTestDriver() {
      Properties config = new Properties();
      config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
      config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
      config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
      config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
      config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
      config.put(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
      config.put(ProducerConfig.RETRIES_CONFIG, "3");
      config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
      BankBalanceExactlyOnce bankBalanceExactlyOnce = new BankBalanceExactlyOnce();

      Topology topology = bankBalanceExactlyOnce.createTopology();
      testDriver = new TopologyTestDriver(topology, config);
    }

    @After
    public void closeTestDriver() {
      testDriver.close();
    }

    public void pushNewInputRecord(String key, String value){
      testDriver.pipeInput(recordFactory.create("bank-balance-input", key, value));
    }

    public ProducerRecord<String, String> readOutput(){
      return testDriver.readOutput("bank-balance-output", new StringDeserializer(), new StringDeserializer());
    }

  private static JsonNode createTransactionRecord(String name, String value){
    ObjectMapper transactionMapper = new ObjectMapper();
    ObjectNode transaction = transactionMapper.createObjectNode();

    transaction.put("Name", name);
    transaction.put("Amount", value);
    transaction.put("Time", Instant.parse("2018-08-19T16:02:42.00Z").toString());

    return transaction;
  }

  @Test
    public void bankBalanceTest() throws IOException {
      String user1Balance = "Alice";
      String user1BalanceAmount = "20";
      String user2Balance = "Bob";
      String user2BalanceAmount = "30";
      String user3Balance = "John";
      String user3BalanceAmount = "40";

      ObjectMapper amountMapper = new ObjectMapper();
      String path = "src/test/resources/json-files/";

      File aliceAmount1JsonFile = new File(path + "alice_amount_1.json");
      File aliceAmount2JsonFile = new File(path + "alice_amount_2.json");
      File bobAmount1JsonFile = new File(path + "bob_amount_1.json");
      File bobAmount2JsonFile = new File(path + "bob_amount_2.json");
      File johnAmount1JsonFile = new File(path + "john_amount_1.json");
      File johnAmount2JsonFile = new File(path + "john_amount_2.json");

      JsonNode aliceAmount1 = amountMapper.readTree(aliceAmount1JsonFile);
      JsonNode aliceAmount2 = amountMapper.readTree(aliceAmount2JsonFile);
      JsonNode bobAmount1 = amountMapper.readTree(bobAmount1JsonFile);
      JsonNode bobAmount2 = amountMapper.readTree(bobAmount2JsonFile);
      JsonNode johnAmount1 = amountMapper.readTree(johnAmount1JsonFile);
      JsonNode johnAmount2 = amountMapper.readTree(johnAmount2JsonFile);

      pushNewInputRecord(user1Balance, createTransactionRecord(user1Balance, user1BalanceAmount).toString());
      OutputVerifier.compareKeyValue(readOutput(), "Alice", aliceAmount1.toString());
      pushNewInputRecord(user2Balance, createTransactionRecord(user2Balance, user2BalanceAmount).toString());
      OutputVerifier.compareKeyValue(readOutput(), "Bob", bobAmount1.toString());
      pushNewInputRecord(user3Balance, createTransactionRecord(user3Balance, user3BalanceAmount).toString());
      OutputVerifier.compareKeyValue(readOutput(), "John", johnAmount1.toString());
      
      pushNewInputRecord(user1Balance, createTransactionRecord(user1Balance, user1BalanceAmount).toString());
      OutputVerifier.compareKeyValue(readOutput(), "Alice", aliceAmount2.toString());
      pushNewInputRecord(user2Balance, createTransactionRecord(user2Balance, user2BalanceAmount).toString());
      OutputVerifier.compareKeyValue(readOutput(), "Bob", bobAmount2.toString());
      pushNewInputRecord(user3Balance, createTransactionRecord(user3Balance, user3BalanceAmount).toString());
      OutputVerifier.compareKeyValue(readOutput(), "John", johnAmount2.toString());

      assertNull(readOutput());
    }
}
