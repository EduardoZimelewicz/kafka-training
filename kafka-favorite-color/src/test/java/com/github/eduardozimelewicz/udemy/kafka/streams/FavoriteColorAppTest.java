package com.github.eduardozimelewicz.udemy.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
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

public class FavoriteColorAppTest {
    private TopologyTestDriver testDriver;

    private final StringSerializer stringSerializer = new StringSerializer();

    private final ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    @Before
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        FavoriteColorApp favoriteColorApp = new FavoriteColorApp();
        Topology topology = favoriteColorApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecord(String value){
        testDriver.pipeInput(recordFactory.create("favorite-color-input", null, value));
    }

    public ProducerRecord<String, Long> readOutput(){
        return testDriver.readOutput("favorite-color-output", new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void dummyTest(){
        String dummyColor = "bob,blue";
        pushNewInputRecord(dummyColor);
        OutputVerifier.compareKeyValue(readOutput(), "blue", 1L);

        assertNull(readOutput());
    }

    @Test
    public void valueRegexMatchTest(){
        String firstUserColor = "bob,blue";
        pushNewInputRecord(firstUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "blue", 1L);

        String secondUserColor = "alice!,red";
        pushNewInputRecord(secondUserColor);
        assertNull(readOutput());

        String thirdUserColor = "janet,green$";
        pushNewInputRecord(thirdUserColor);
        assertNull(readOutput());

        String forthUserColor = "neil@,green#";
        pushNewInputRecord(forthUserColor);
        assertNull(readOutput());

    }

    @Test
    public void lowercaseFunctionTest(){
        String firstUserColor = "BOB,BLUE";
        pushNewInputRecord(firstUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "blue", 1L);
    }

    @Test
    public void colorFilterTest(){
        String firstUserColor = "dumb,yellow";
        pushNewInputRecord(firstUserColor);
        assertNull(readOutput());

        String secondUserColor = "smart,green";
        pushNewInputRecord(secondUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "green", 1L);

        String thirdUserColor = "smart2,blue";
        pushNewInputRecord(thirdUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "blue", 1L);

        String forthUserColor = "smart3,red";
        pushNewInputRecord(forthUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "red", 1L);

        String fifthUserColor = "dumb2,brown";
        pushNewInputRecord(fifthUserColor);
        assertNull(readOutput());
    }

    @Test
    public void favoriteColorTableTest(){
        String firstUserColor = "bob,blue";
        pushNewInputRecord(firstUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "blue", 1L);

        String secondUserColor = "john,green";
        pushNewInputRecord(secondUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "green", 1L);

        String thirdUserColor = "bob,red";
        pushNewInputRecord(thirdUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "blue", 0L);
        OutputVerifier.compareKeyValue(readOutput(), "red", 1L);

        String forthUserColor = "alice,red";
        pushNewInputRecord(forthUserColor);
        OutputVerifier.compareKeyValue(readOutput(), "red", 2L);
    }

}
