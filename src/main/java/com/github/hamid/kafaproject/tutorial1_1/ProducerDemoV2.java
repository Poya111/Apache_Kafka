package com.github.hamid.kafaproject.tutorial1_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemoV2 {
    public static void main(String[] args) {

        String topicName = "student";
        String bootstarpServer = "127.0.0.1:9092";
        String value = "Hello World Kafka in another way";
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

        //Send stream to Kafka
        producer.send(record);

        //Close Producer
        producer.close();

    }
}
