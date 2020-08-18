package com.github.hamid.kafaproject.tutorial2_2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemoCallbackV2 {
    public static void main(String[] args) {

        String topicName = "student";
        String bootstarpServer = "127.0.0.1:9092";
        String value = "Hello World Kafka with Asynchronous callback.";
        String key = "";

        //Create Properties Producer
        Properties properties =  new Properties();
        properties.put("bootstrap.servers", bootstarpServer);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Create Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create Record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, value);

        /*Sending stream to Kafka with Asynchronous Callback makes higher throughput, but it has a limitation, 5
        messages in flight by default.
        */

        producer.send(record, new MyProducerCallback());

        //Close Producer
        producer.close();

    }
}

