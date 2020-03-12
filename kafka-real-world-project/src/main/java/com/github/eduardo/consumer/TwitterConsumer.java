package com.github.eduardo.consumer;

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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localvm", 9200, "http")));

        int tweetsToRead = 10;

        //while (tweetsToRead > 0) {
            //Deprecated
            //consumer.poll(1000);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            int id = 1;

            for (ConsumerRecord<String, String> record : records) {

                logger.info("Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                IndexRequest request = new IndexRequest("posts");
                request.id(Integer.toString(id));
                request.source(record.value(), XContentType.JSON);

                try {
                    IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
                    logger.info("Result: " + indexResponse.getResult().toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }

                id += 1;
            }

          //  tweetsToRead += 1;
        //}

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}