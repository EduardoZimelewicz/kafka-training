package com.github.eduardozimelewicz.udemy.kafka.streams;

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

import java.util.Properties;

import static org.junit.Assert.assertNull;

public class UserPurchasesAppTest {
    private TopologyTestDriver testDriver;

    private final StringSerializer stringSerializer = new StringSerializer();

    private final ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    @Before
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        config.put(ProducerConfig.RETRIES_CONFIG, "3");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        UserPurchasesApp userPurchasesApp = new UserPurchasesApp();
        Topology topology = userPurchasesApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecordUserPurchase(String key, String value){
        testDriver.pipeInput(recordFactory.create("user-purchases-input", key, value));
    }

    public void pushNewInputRecordPurchaseData(String key, String value){
        testDriver.pipeInput(recordFactory.create("purchase-data-input", key, value));
    }

    public ProducerRecord<String, String> readInnerJoinOutput(){
        return testDriver.readOutput("user-inner-join-output", new StringDeserializer(), new StringDeserializer());
    }

    public ProducerRecord<String, String> readLeftJoinOutput(){
        return testDriver.readOutput("user-left-join-output", new StringDeserializer(), new StringDeserializer());
    }

    @Test
    public void userLeftJoinTest(){
        String user1purchaseKey = "alice";
        String user1purchaseValue = "microwave";
        String purchase1Value = "100";
        pushNewInputRecordPurchaseData(user1purchaseKey, purchase1Value);
        pushNewInputRecordUserPurchase(user1purchaseKey, user1purchaseValue);

        OutputVerifier.compareKeyValue(readLeftJoinOutput(),"alice","microwave, 100");

        String user2purchaseKey = "bob";
        String user2purchaseValue = "television";
        String purchase2Value = "200";
        pushNewInputRecordPurchaseData(user2purchaseKey, purchase2Value);
        pushNewInputRecordUserPurchase(user2purchaseKey, user2purchaseValue);

        OutputVerifier.compareKeyValue(readLeftJoinOutput(),"bob","television, 200");

        String user3purchaseKey = "ed";
        String user3purchaseValue = "laptop";
        pushNewInputRecordUserPurchase(user3purchaseKey, user3purchaseValue);

        OutputVerifier.compareKeyValue(readLeftJoinOutput(),"ed","laptop, " + null);

        assertNull(readLeftJoinOutput());
    }

    @Test
    public void userInnerJoinTest(){
        String user1purchaseKey = "alice";
        String user1purchaseValue = "microwave";
        String purchase1Value = "100";
        pushNewInputRecordPurchaseData(user1purchaseKey, purchase1Value);
        pushNewInputRecordUserPurchase(user1purchaseKey, user1purchaseValue);

        OutputVerifier.compareKeyValue(readInnerJoinOutput(),"alice","microwave, 100");

        String user2purchaseKey = "bob";
        String user2purchaseValue = "television";
        String purchase2Value = "200";
        pushNewInputRecordPurchaseData(user2purchaseKey, purchase2Value);
        pushNewInputRecordUserPurchase(user2purchaseKey, user2purchaseValue);

        OutputVerifier.compareKeyValue(readInnerJoinOutput(),"bob","television, 200");

        String user3purchaseKey = "ed";
        String user3purchaseValue = "laptop";
        pushNewInputRecordUserPurchase(user3purchaseKey, user3purchaseValue);

        assertNull(readInnerJoinOutput());
    }

}
