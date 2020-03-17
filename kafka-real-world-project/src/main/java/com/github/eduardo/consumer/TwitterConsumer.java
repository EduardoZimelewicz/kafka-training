package com.github.eduardo.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);

    private static String extractIdFromTweet(String tweetJson){
        JsonParser parser = new JsonParser();
        return parser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) {
        Properties properties = new Properties();

        String bootstrapServers = "localvm:9092";
        String groupId = "my-second-twitter--application";
        String topic = "tweet_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localvm", 9200, "http")));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {

            String id = extractIdFromTweet(record.value());

            logger.info("Value: " + record.value());
            logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

            IndexRequest request = new IndexRequest("posts");
            request.id(id);
            request.source(record.value(), XContentType.JSON);

            try {
                IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
                logger.info("Result: " + indexResponse.getResult().toString());
                logger.info("Id: " + id);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        //logger.info("Committing offsets...");
        //consumer.commitSync();
        //logger.info("Offsets have been committed");

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}