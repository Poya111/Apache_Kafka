package com.github.hamid.elasticsearch.tutorial1;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient(){


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


        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }



    public static void main(String[] args) throws IOException {
        System.out.println("hi");

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        //String jsonString = "{\"foo6\": \"bar6\"}";

        String jsonString = "{" +
                "\"user\":\"mari\"," +
                "\"postDate\":\"2020-01-01\"," +
                "\"message\":\"Elasticsearch!!!\"" +
                "}";

        IndexRequest indexRequest = new IndexRequest("twitter", "tweet");
        //indexRequest.id("1");
        indexRequest.source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        String index = indexResponse.getIndex();

        logger.info(id);

        System.out.println("id:" + id + "\n"+"index:" + index);

        client.close();

    }
}
