package com.twitter.kafka.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TweetProducer {
    static String topicName = "";
    static List<String> terms = new ArrayList<>();
    Logger logger = LoggerFactory.getLogger(TweetProducer.class.getName());
    String consumerKey = "";
    String consumerSecret = "";
    String token = "";
    String secret = "";

    public static void main(String[] args) throws JSONException, IOException {
        Scanner in = new Scanner(System.in);
        System.out.println("Enter hashtag on Twitter to Follow");
        String topic = in.next();
        System.out.println("Enter Kafka Topic to Write");
        terms.add(topic);
        topicName = in.next();
        new TweetProducer().run();
    }

    public void run() throws JSONException, IOException {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping Application");
            System.out.println("Shutting Twitter Streaming Client");
            client.stop();
            System.out.println("Close Producer");
            producer.close();
            System.out.println("Done");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                String tweetId = new JSONObject(msg).getString("id");
                System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
                System.out.println(new JSONObject(msg).getString("text")
                        .replace("\n", " ")
                        .replace("\t", " "));
                System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++\n");

                producer.send(new ProducerRecord<>(topicName, tweetId, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        System.out.println("Unable to Produce the Message" + e);
                    } else {
                        System.out.println("Topic :" + recordMetadata.topic());
                        System.out.println("Partition :" + recordMetadata.partition());
                        System.out.println("Offset :" + recordMetadata.offset());
                        System.out.println("TimeStamp :" + recordMetadata.timestamp());
                    }
                });
            }
        }
        System.out.println("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }
}