package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    static void main() {
        log.info("I am a Kafka Producer");

        System.out.println("Hello World!");

        // create Producer Properties

        // connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("batch.size", "400");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
            // kafka producer with key as string, and value as string
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            // create a producer record
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo.java", "HEY WORLD!, PRODUCER WITH CALLBACK");

        for(int i=0; i<10; i++) {
            for(int j=0; j<30; j++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo.java", "Hello World"+j);
                // send Data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown.
                        if(e == null) {
                            // The record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n"+
                                    "Partition: " + recordMetadata.partition() + "\n"+
                                    "Offset: " + recordMetadata.offset() + "\n"+
                                    "Timestamp: " + recordMetadata.timestamp() + "\n"
                            );

                        } else {
                            log.error("Error while Producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        // tell the producer to send all data and block until done --synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
