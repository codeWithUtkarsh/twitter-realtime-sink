package com.codewithutkarsh.producer.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.spi.CleanableThreadContextMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TwitterProducer {

	private static Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);

	private String consumerKey = "";
	private String consumerSecret = "";
	private String token = "";
	private String secret = "";

	private static String bootstrapServer = "localhost:29092";

	public TwitterProducer() {}

	public static void main(String args[]) {
		new TwitterProducer().run();
	}

	public void run() {

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create twitter client
		final Client client = createTwitterClient(msgQueue);
		client.connect();

		// create kafka producer
		final KafkaProducer<String, String> kafkaProducer = getKafkaProducer();

		//adding runtime hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			LOGGER.info("Shuting down application");
			LOGGER.info("Closing twitter client");
			client.stop();
			LOGGER.info("Closing producer");
			kafkaProducer.close();
		}));
		
		// loop to send tweets to kafka
		while (!client.isDone()) {

			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}

			if (msg != null) {

				LOGGER.info(msg);
				kafkaProducer.send(
						new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {

							public void onCompletion(RecordMetadata metadata, Exception exception) {

								if(exception != null) {

									LOGGER.error("Something went wrong");
								}
							}
						});
				
			}
		}
	}

	private KafkaProducer<String, String> getKafkaProducer() {

		//create config
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<String, String>(properties);
	}

	private Client createTwitterClient(BlockingQueue<String> msgQueue) {

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
}
