package com.github.hamid;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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

public class ElasticSearchConsumerFromTwitterIdempotenceMancommit {

    public static RestHighLevelClient createClient(){

        //Initiate credential ElasticSearch
        String hostname = "kafka-course-elastic-4878268790.us-east-1.bonsaisearch.net";
        String username = "pm4n1fzzt1";
        String password = "bej86cj6tr";


        //Don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    //@Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });


        //Create Client
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        //Initiate bootstrap, GroupId
        String bootstrapservers = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch-demo";

        //Config Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    //Using gson Library, jackson Library for parsing.
    private  static JsonParser jsonParser = new JsonParser();
    private static String extractIdTweet(String jsonTweet){

        return jsonParser.parse(jsonTweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }



    public static void main(String[] args) throws IOException {
        //Create Logger
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumeronCloud.class.getName());

        //Create Client
        RestHighLevelClient client = createClient();

        //Create json format
        //String jsonString= "{\"foo19\" : \"bar19\"}";

        //Create consumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter");

        //Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received: " + records.count() + "records.");

            for (ConsumerRecord<String, String> record : records) {

                //Where insert stream to ElasticSearch
                //Create json format
                String jsonString = record.value();

                //By default delivery semantic is "at least once" to prevent lost of data when consumer
                //become down.
                //Using Idempotence to prevent from duplicate data on sink such as ElasticSearch via id.
                //There are two strategies for create unique id, kafka Generic id.
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset
                String id = extractIdTweet(record.value());

                //Create indexRequest
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id);
                indexRequest.source(jsonString, XContentType.JSON);

                //Create indexResponse
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                //String id = indexResponse.getId();
                String index = indexResponse.getIndex();

                //Log id
                logger.info(indexResponse.getId());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
            logger.info("Committing offset.... ");
            consumer.commitSync();
            logger.info("offsets have been committed.");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Close Client
        //client.close();
    }

}
