package com.github.hamid.kafaproject.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello World Kafka!!!!");
        String bootstrapServers = "127.0.0.1:9092";
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String,String> producer =new KafkaProducer<String, String>(properties);

        // Producer Recorde
        ProducerRecord<String , String> record =
                new ProducerRecord<String, String>("student", "Hello World Kafka Producer for 3 consumers#@$^&!!!!");
        // Send Data
        producer.send(record);

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
