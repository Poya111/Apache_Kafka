package hamid.kafaproject.tutorial3;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

        String topic = " ";
        String value = " ";
        String key = " ";

        System.out.println("Hello World Kafka!!!!");
        String bootstrapServers = "127.0.0.1:9092";
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Send messages 10 time & check the same message go to the same partition.
        for (int i = 0; i < 20; i++) {

            topic = "student";
            value = "Hello World Kafka Producer with Key_" + Integer.toString(i);
            key = "id_" + Integer.toString(i);

            // Producer Recorde
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);


            // Send Data Asynchronous
            final String finalKey = key;
            producer.send(record, new Callback() {
                //Execute every time a record successfully is sent or thrown an exception.
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        //log info for key
                        logger.info("key: " + finalKey);

                        //Log metadata when the record was successfully sent.
                        logger.info(" \nTopic:" + recordMetadata.topic() + "\n" +
                                        "Partitions: " + recordMetadata.partition() + "\n" +
                                        "Offsets: " + recordMetadata.offset() + "\n" +
                                        "TimeStamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("error while producing.");
                        // or StackTrace instead of logger
                        //e.printStackTrace();
                    }
                }
            });
        }
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
