package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    static void main() {
        log.info("I am a Kafka Producer");

        System.out.println("Hello World!");

        // create Producer Properties

        // connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
            // kafka producer with key as string, and value as string
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            // create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo.java", "hello world");

        // send Data
        producer.send(producerRecord);


        // tell the producer to send all data and block until done --synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
