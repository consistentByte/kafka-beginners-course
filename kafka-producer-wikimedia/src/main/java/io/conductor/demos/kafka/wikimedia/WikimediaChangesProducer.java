package io.conductor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.launchdarkly.eventsource.EventHandler;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    static void main(String args[]) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer Properties
        // connect to localhost
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set safe producer configs (Kafka <= 2.8)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // same as setting -1
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");



        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        Map<String, String> headers = new HashMap<>();
        headers.put("User-Agent", "MyKafkaApp/1.0 (sarvajanik87@gmail.com)");

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url)).headers(Headers.of(headers)); // handler and url

        EventSource eventSource = builder.build();

        // Start the producer in another thread
        eventSource.start(); // to start the event handling

        // We produce for 10 minuts and block the program until then
        TimeUnit.MINUTES.sleep(10);

    }


}
