package com.github.hamid.kafaproject.tutorial2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
                new ProducerRecord<String, String>("student", "Hello World Kafka Producer with callback!!!");
        // Send Data Asynchronous
        producer.send(record, new Callback() {
            //Execute every time a record successfully is sent or thrown an exception.
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    //Log metadata when the record was successfully sent.
                    logger.info(" \n Topic:" + recordMetadata.topic() + "\n" +
                            "Partitions: " + recordMetadata.partition() + "\n" +
                            "Offsets: " + recordMetadata.offset() + "\n" +
                            "TimeStamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("error while producing.");
                }
            }
        });

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
