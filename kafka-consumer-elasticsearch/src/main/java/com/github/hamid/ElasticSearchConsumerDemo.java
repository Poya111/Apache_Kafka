package com.github.hamid;

import org.apache.http.HttpHost;
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

public class ElasticSearchConsumerDemo {

    public static RestHighLevelClient createClient(){

        //Using localhost ElasticSearch
        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerDemo.class.getName());

        RestHighLevelClient client = createClient();

        String jsonString = "{\"foo16\" : \"bar16\"}";
        IndexRequest indexRequest = new IndexRequest("twitter", "tweet");
        indexRequest.source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id =indexResponse.getId();
        String index = indexResponse.getIndex();

        logger.info(id);

        client.close();


    }
}
