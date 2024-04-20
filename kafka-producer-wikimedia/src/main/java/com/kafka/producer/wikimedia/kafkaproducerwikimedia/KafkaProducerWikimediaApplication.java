package com.kafka.producer.wikimedia.kafkaproducerwikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.kafka.producer.wikimedia.kafkaproducerwikimedia.events.WikiMediaEventHandler;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerWikimediaApplication {

	public static void main(String[] args) throws InterruptedException{

		String bootstrapServers = "localhost:9092";

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create a producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		String topic = "wikimedia.recentchange";

		// event handling
		EventHandler eventHandler = new WikiMediaEventHandler(producer, topic);

		String url = "https://stream.wikimedia.org/v2/stream/recentchange";

		EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
		EventSource eventSource = builder.build();

		// start the producer in another thread
		eventSource.start();

		// producer for 1-min and block
		TimeUnit.SECONDS.sleep(30);

		// acks = 0  :producer is not going to wait for acknowledgment (possible loss of data)
		// acks = 1  :producer is going to wait for broker leader acknowledgment (limited loss of data)
		// acks = -1 :producer is going to wait for leader + replicas to acknowledgment (no data loss)

	}

}
