package com.poc.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.poc.kafka.model.PersonModel;

/**
 * In order to successfully send messages to a Kafka topic, we need to configure
 * The KafkaTemplate. This configuration is handled by the SenderConfig class.
 * 
 * @author kali
 *
 */
@Configuration
public class SenderConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		// Specifies a list of host/port pairs to use for establishing the initial
		// connection to the Kafka cluster.
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		// Specifies the serializer class for key that implements the
		// org.apache.kafka.common.serialization.Serializer interface.
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

		// ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG specifies the serializer class
		// for value that implements the
		// org.apache.kafka.common.serialization.Serializer interface.
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return props;
	}

	@Bean
	public ProducerFactory<String, PersonModel> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}
	
	@Bean
	public KafkaTemplate<String, PersonModel> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}
