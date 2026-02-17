package io.conducktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        // Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();

    }
    static void main() throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

        // first create an Opensearch client
        RestHighLevelClient openSearchClient =  createOpenSearchClient();

        // create our kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // we need to create an index on OpenSearch if it doesn't exist already
        try (openSearchClient; consumer){
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),  RequestOptions.DEFAULT);
            // if something goes wrong, this try block will automatically close the openSearchClient
            if(!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

                log.info("The wikimedia index has been created");
            } else {
                log.info("wikimedia index already exists");
            }

            // subscribe to a topic
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            // not adding shutdown hooks to focus on consumer part
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received "+ recordCount + "record(s)");

                BulkRequest bulkRequest = new BulkRequest();

                // Now we can send these records to elastic search/opensearch.

                for(ConsumerRecord<String, String> record: records) {
                    // send the record into OpenSearch

                    //strategy 1
                    // define an ID using Kafka Record coordinates
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // Strategy 2
                    // if we are getting an ID from messages use that.


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

                    try {
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        // Since now we have provided, an id, if we sent an id again,
                        // elastic search will update the one in place since now we have provided an id.

                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);
//                        log.info(response.getId());
                    }catch(WakeupException e){
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

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length+ " record(s)");

                    try {
                        // to sleep for 1 sec between every call
                        Thread.sleep(1000);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                // after the whole batch is consumed, then we can commit offsets
                consumer.commitSync(); // can be commitAsync as well
                log.info("Offsets have been committed");
            }
        }
        // main code logic

        // close things


    }
}
