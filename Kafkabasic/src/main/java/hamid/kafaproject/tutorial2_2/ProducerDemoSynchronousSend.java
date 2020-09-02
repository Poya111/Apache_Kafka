package hamid.kafaproject.tutorial2_2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ProducerDemoSynchronousSend {
    public static void main(String[] args)  {

        String topicName = "student";
        String bootstarpServer = "127.0.0.1:9092";
        String value = "Hello World Kafka synchronous!";
        String key = "";

        //Create Properties Producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstarpServer);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Create Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create Record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, value);

        /*
        For synchronous data, try and catch is used if exception occur. This approach checks everytime the message
        is sent. As a result, throughput is slower than other strategies.
        */
        try {
            //Send stream to Kafka
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Topic: " + metadata.topic() + "Message sent to partition No.: " + metadata.partition() + "\n" +
                    "Timestamp: " + metadata.timestamp() + "Offset: " + metadata.offset());
            System.out.println("Synchronous Producer completed successfully.");

        }catch(Exception e) {
            e.printStackTrace();
            System.out.println("Synchronous Success failed.");

        }finally{
            //Close Producer
            producer.close();
            System.out.println("Producer closed successfully.");
        }
    }

}





