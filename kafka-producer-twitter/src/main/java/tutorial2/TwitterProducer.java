package tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.io.FileNotFoundException;


public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer(){};

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Setup");

        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stop application...");
            twitterClient.stop();
            producer.close();
            logger.info("closed application...exit...");
        }));

        // loop tweets to send to kafka
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }

            if (msg != null) {
                logger.info(msg);

                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("twitter", msg);

                // send records to producer asynchronously
                producer.send(record,
                        new Callback() {
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if (e == null) {
                                    // the records is sent successfully
                                    logger.info("Received new metdata. \n" +
                                            "Topic:" + recordMetadata.topic() + "\n" +
                                            "Partition:" + recordMetadata.partition() + "\n" +
                                            "Timestamp:" + recordMetadata.timestamp());
                                } else {
                                    logger.error("Error while producing", e);
                                }
                            }
                        });
            }

            logger.info("Ending of application");
        }
    }

    public KafkaProducer<String, String> createKafkaProducer(){

        // create a kafka producer

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;

    }

    public Client createTwitterClient(BlockingQueue msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("covid");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        JSONObject twitterSecrets = readSecrets("secrets.json");

        String consumerKey = (String) twitterSecrets.get("api_key");
        String consumerSecret = (String) twitterSecrets.get("api_secret_key");
        String token = (String) twitterSecrets.get("token");
        String tokenSecret = (String) twitterSecrets.get("tokenSecret");

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }

    public JSONObject readSecrets(String filename){

        //example snippet: https://howtodoinjava.com/library/json-simple-read-write-json-examples/

        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();
        JSONObject secretObject = new JSONObject();

        try
        {
            FileReader reader = new FileReader(filename);

            //Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONArray secretList = (JSONArray) obj;
            secretObject = (JSONObject) secretList.get(0);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return secretObject;

    }

}
