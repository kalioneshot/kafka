package com.poc.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.poc.kafka.model.PersonModel;

/**
 * This mechanism requires an @EnableKafka annotation on one of
 * the @Configuration classes and listener container factory, which is used to
 * configure the underlying ConcurrentMessageListenerContainer.
 * 
 * @author kali
 *
 */
@EnableKafka
@Configuration
public class ReceiverConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		// Specifies a unique string that identifies the consumer group this consumer
		// belongs to.
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "json");
		// Specifies what to do when there is no initial offset in Kafka or if the
		// current offset does not exist any more on the server (e.g. because that data
		// has been deleted):
		// - earliest: automatically reset the offset to the earliest offset
		// - latest: automatically reset the offset to the latest offset
		// - none: throw exception to the consumer if no previous offset is found for
		// the consumer’s group
		// - anything else: throw exception to the consumer.
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return props;
	}

//	@Bean
//	public ConsumerFactory<String, String> consumerFactory() {
//		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
//	}
//
//	@Bean
//	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
//		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//		factory.setConsumerFactory(consumerFactory());
//		return factory;
//	}

	@Bean
	public ConsumerFactory<String, PersonModel> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
				new JsonDeserializer<>(PersonModel.class));
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, PersonModel>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, PersonModel> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}

}
