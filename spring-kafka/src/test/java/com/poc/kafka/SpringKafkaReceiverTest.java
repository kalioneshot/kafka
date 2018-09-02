package com.poc.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.poc.kafka.consumer.Receiver;
import com.poc.kafka.model.PersonModel;

/**
 * The Spring Kafka project comes with a spring-kafka-test JAR that contains a
 * number of useful utilities to assist you with your application unit testing.
 * These include an embedded Kafka server, some static methods to setup
 * consumers/producers and utility methods to fetch results.
 * 
 * In order to have the correct broker address set on the Sender and Receiver
 * beans during each test case we need to use the @DirtiesContext on all test
 * classes. The reason for this is that each test case contains its own embedded
 * Kafka broker that will each be created on a new random port. By rebuilding
 * the application context, the beans will always be set with the current broker
 * address.
 * 
 * @author kali
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SpringKafkaReceiverTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpringKafkaReceiverTest.class);
	private static String RECEIVER_TOPIC = "receiver.t";

	//private static String RECEIVER_TOPIC = "jsonKafka";
	@Autowired
	private Receiver receiver;

	/**
	 * The producer properties are created using the static senderProps() method
	 * provided by KafkaUtils. These properties are then used to create a
	 * DefaultKafkaProducerFactory which is in turn used to create a KafkaTemplate.
	 * Finally we set the default topic that the template uses to 'receiver.t'
	 */
	private KafkaTemplate<String, PersonModel> template;

	/**
	 * The link to the message listener container is acquired by auto-wiring the
	 * KafkaListenerEndpointRegistry which manages the lifecycle of the listener
	 * containers that are not created manually.
	 */
	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	/**
	 * spring-kafka-test includes an embedded Kafka server that can be created via a
	 * JUnit @ClassRule annotation. The rule will start a ZooKeeper and Kafka server
	 * instance on a random port before all the test cases are run, and stops the
	 * instances once the test cases are finished.
	 * 
	 * The KafkaEmbedded constructor takes as parameters: the number of Kafka
	 * servers to start, whether a controlled shutdown is needed and the topics that
	 * need to be created on the server.
	 * 
	 * Always pass the topics as a parameter to the embedded Kafka server. This
	 * assures that the topics are not auto-created and present when the
	 * MessageListener connects
	 */
	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, RECEIVER_TOPIC);

	@Before
	public void setUp() throws Exception {

		// Set up the Kafka producer properties.
		Map<String, Object> senderProperties = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

		// Create a Kafka producer factory.
		ProducerFactory<String, PersonModel> producerFactory = new DefaultKafkaProducerFactory<String, PersonModel>(
				senderProperties);

		// Create a Kafka template.
		template = new KafkaTemplate<>(producerFactory);

		// Set the default topic to send to.
		template.setDefaultTopic(RECEIVER_TOPIC);

		// Wait until the partitions are assigned.
		for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
				.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
		}
	}

	@Test
	public void testReceive() throws Exception {
		// Send the message.
		PersonModel person = new PersonModel("Roignant__", "Cédric__");
		template.send(RECEIVER_TOPIC, person);
		LOGGER.debug("test-sender sent message='{}'", person);

		receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);

		// Check that the message was received
		assertThat(receiver.getLatch().getCount()).isEqualTo(0);
	}

}
