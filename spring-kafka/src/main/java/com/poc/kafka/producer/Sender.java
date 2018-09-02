package com.poc.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.poc.kafka.model.PersonModel;

/**
 * Spring Kafka Sending Messages to Topic
 * 
 * @author kali
 *
 */
@Service
public class Sender {

	private static final Logger logger = LoggerFactory.getLogger(Sender.class);

	/*
	 * We use the KafkaTemplate class which wraps a Producer and provides high-level
	 * operations to send data to Kafka topics.
	 */
	@Autowired
	private KafkaTemplate<String, PersonModel> kafkaTemplate;

	@Value("${kafka.topic.json}")
	private String topic;

	public void send(String topic, PersonModel message) {
		logger.info("sending message='{}' to topic='{}'", message.toString(), topic);
		kafkaTemplate.send(topic, message);
	}

	public void send(PersonModel message) {
		logger.info("###### sending message='{}' to topic='{}'", message.toString(), topic);
		kafkaTemplate.send(topic, message);
	}

}
