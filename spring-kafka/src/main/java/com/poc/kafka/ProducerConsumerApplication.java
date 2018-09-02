package com.poc.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.poc.kafka.model.PersonModel;
import com.poc.kafka.producer.Sender;

/**
 * Finally, we wrote a simple Spring Boot application to demonstrate the
 * application. In order for this demo to work, we need a Kafka Server running
 * on localhost on port 9092, which is the default configuration of Kafka.
 * 
 * # Get consumer from topic
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic jsonKafka –from-beginning
 * 
 * # Start kafka server.
 * bin/kafka-server-start.sh config/server.properties
 *
 * # Start Zookeeper server.
 * bin/zookeeper-server-start.sh config/zookeeper.properties
 *
 * @author kali
 *
 */
@SpringBootApplication
public class ProducerConsumerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(ProducerConsumerApplication.class, args);
	}

	@Value("${kafka.topic.json}")
	private String topic;

	@Autowired
	private Sender senderPerson;
	
	@Override
	public void run(String... strings) throws Exception {
		
		//1.
		//sender.send("Spring Kafka Producer and Consumer Example - " + new Date());

		//2.
		PersonModel person = new PersonModel("Roignant","Cédric");
		senderPerson.send(topic, person);
	}

}
