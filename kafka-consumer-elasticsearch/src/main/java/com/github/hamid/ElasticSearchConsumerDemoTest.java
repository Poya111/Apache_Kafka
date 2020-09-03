package com.github.hamid;

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
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerDemoTest {

    public static RestHighLevelClient createClient(){

        //Using localhost ElasticSearch
        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrapservers = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch-demo";

        //Config Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);


        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to topic(s)
        consumer.subscribe(Arrays.asList(topic));


        return consumer;
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerDemoTest.class.getName());

        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer("twitter");


        //Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));


            for (ConsumerRecord<String, String> record : records) {

                String jsonString = record.value();
                //String jsonString = "{\"foo22\" : \"bar22\"}";

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets");
                indexRequest.source(jsonString, XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id =indexResponse.getId();
                String index = indexResponse.getIndex();

                logger.info(id);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //client.close();
    }
}
