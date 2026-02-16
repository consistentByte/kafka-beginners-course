package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    static void main() {
        log.info("I am a Kafka Producer");

        String groupId = "my-java-application";
        String topic = "demo.java";

        // create consumer Properties
        // connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // Set id of Consumer Group, if consumer group does not exist, fail, so set the consumer group before starting the app
        properties.setProperty("group.id", groupId);

        // offset reset
        properties.setProperty("auto.offset.reset", "earliest");

        // Create a Consumer
            // kafka consumer with key as string, and value as string
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//            consume record

//            in subscribe, pass in pattern or a collection of topics.

        consumer.subscribe(Arrays.asList(topic));

        // keep on pulling data infinitely
        while(true) {
            log.info("Polling");
            ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records) {
                log.info("Key: "+ record.key()+", Value: "+ record.value());
                log.info("Partition: "+ record.partition()+", Offset: "+ record.offset());
            }
        }
    }
}
