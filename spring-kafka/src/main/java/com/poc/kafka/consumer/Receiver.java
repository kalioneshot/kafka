package com.poc.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.poc.kafka.model.PersonModel;

/**
 * Spring Kafka Listening Messages from Topic.
 * 
 * The Receiver class will consume messages form a Kafka topic. We created the
 * Listen() method and annotated it with the @KafkaListener annotation which
 * marks the method to be the target of a Kafka message listener on the
 * specified topics.
 * 
 * @author kali
 *
 */
@Service
public class Receiver {

	private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

	private CountDownLatch latch = new CountDownLatch(5);

	public CountDownLatch getLatch() {
		return latch;
	}

	@KafkaListener(topics = "${kafka.topic.json}")
	public void listen(@Payload PersonModel record) {
		logger.info("##### received message='{}'", record.toString());
		getLatch().countDown();
	}

}
