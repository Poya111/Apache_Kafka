package hamid.kafaproject.tutorial2_2;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        //Not Success Callback
        if (e != null){
            System.out.println("Asynchronous Unsuccessful Callback.");
        }else{
            System.out.println("Asynchronous Successful Callback.");
        }

    }
}
