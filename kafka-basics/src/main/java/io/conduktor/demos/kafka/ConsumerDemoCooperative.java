package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

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

        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //Strategy for static assignments
//        properties.setProperty("group.instance.id", ".....");

        // Create a Consumer
            // kafka consumer with key as string, and value as string
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference of main thread
        final Thread mainThread = Thread.currentThread();
        // Adding a Shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // Don't finish yet, wait for the main program to finish.
                // join the main thred to allow the execution of code in the main thread.
                try {
                    mainThread.join();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }) ;


//            consume record
//            in subscribe, pass in pattern or a collection of topics.
        consumer.subscribe(Arrays.asList(topic));


        // Wrapping in try catch since we know, at some point, conusmer.poll will throw a wake up exception.
        try {
            // keep on pulling data infinitely
            while(true) {
                log.info("Polling");
                ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: records) {
                    log.info("Key: "+ record.key()+", Value: "+ record.value());
                    log.info("Partition: "+ record.partition()+", Offset: "+ record.offset());
                }
            }
        } catch(WakeupException e){
            // If wakeException, we knw that this will be thrown so handled
            log.info("Consumer is starting to shut down");
        } catch(Exception e) {
            // if some other exception, log it.
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close();
            // close the consmer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");
        }
    }
}
