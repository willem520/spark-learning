package simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @Author weiyu005@ke.com
 * @Description
 * @Date 2018/10/31 11:44
 */
public class KafkaProducerSimulator {
    public static final String TOPIC = "test";
    public static final String BOOTSTRAP_SERVER = "10.26.27.81:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serializa-tion.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serializa-tion.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(props);

        Runtime.getRuntime().addShutdownHook(new Thread(()-> kafkaProducer.close()));

        while (true){
            ProducerRecord record = new ProducerRecord<String,String>(TOPIC,"123");
            kafkaProducer.send(record);
        }
    }
}
