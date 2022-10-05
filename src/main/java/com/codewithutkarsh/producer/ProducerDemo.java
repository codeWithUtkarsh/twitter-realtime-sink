package com.codewithutkarsh.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	private static String bootstrapServer = "localhost:29092";
	public static void main(String[] args) {

		//create config
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		
		//create record
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "Hello World");
		
		//send record
		kafkaProducer.send(producerRecord);
		
		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
