package com.poc.kafka;

import static org.assertj.core.api.Assertions.assertThat;
// import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
// import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.poc.kafka.model.PersonModel;
import com.poc.kafka.producer.Sender;

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
public class SpringKafkaSenderTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpringKafkaSenderTest.class);
	private static String SENDER_TOPIC = "sender.t";

	@Autowired
	private Sender sender;

	/**
	 * For creating the needed consumer properties a static consumerProps() method
	 * provided by KafkaUtils is used. We then create a DefaultKafkaConsumerFactory
	 * and ContainerProperties which contains runtime properties (in this case the
	 * topic name) for the listener container. Both are then passed to the
	 * KafkaMessageListenerContainer constructor
	 */
	private KafkaMessageListenerContainer<String, PersonModel> container;

	/**
	 * Received messages need to be stored somewhere. In this example, a thread safe
	 * BlockingQueue is used. We create a new MessageListener and in the onMessage()
	 * method we add the received message to the BlockingQueue.
	 */
	private BlockingQueue<ConsumerRecord<String, PersonModel>> records;

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
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, SENDER_TOPIC);

	@Before
	public void setUp() throws Exception {

		// Set up the Kafka consumer properties.
		Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("sender", "false", embeddedKafka);

		// Create a Kafka consumer factory.
		DefaultKafkaConsumerFactory<String, PersonModel> consumerFactory = new DefaultKafkaConsumerFactory<String, PersonModel>(
				consumerProperties);

		// Set the topic that needs to be consumed.
		ContainerProperties containerProperties = new ContainerProperties(SENDER_TOPIC);

		// Create a Kafka MessageListenerContainer.
		container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		// Create a thread safe queue to store the received message.
		records = new LinkedBlockingQueue<>();

		// Set up a Kafka message listener.
		container.setupMessageListener(new MessageListener<String, PersonModel>() {
			@Override
			public void onMessage(ConsumerRecord<String, PersonModel> record) {
				LOGGER.debug("test-listener received message='{}'", record.toString());
				records.add(record);
			}
		});

		// Start the container and underlying message listener.
		container.start();

		// Wait until the container has the required number of assigned partitions
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
	}

	@After
	public void tearDown() {
		// Stop the container.
		container.stop();
	}

	@Test
	public void testSend() throws InterruptedException {
		// Send the message.
		PersonModel person = new PersonModel("Roignant_", "Cédric_");
		sender.send(SENDER_TOPIC, person);

		// check that the message was received
		ConsumerRecord<String, PersonModel> received = records.poll(10, TimeUnit.SECONDS);

		assertThat(received).isNotNull();
		//assertThat(received.value().getName()).isEqualTo(person.getName());
		// Hamcrest Matchers to check the value
		//assertThat(received, hasValue(person));

		// AssertJ Condition to check the key
		assertThat(received).has(key(null));
	}
}