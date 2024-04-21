package org.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public DefaultErrorHandler errorHandler(){
        var fixedBackoff = new FixedBackOff(1000L, 2);
        var errorHandler = new DefaultErrorHandler();
        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception: {}, deliveryAttempt: {}",
                    ex.getMessage(), deliveryAttempt);
        }));
        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        // will spin 3 threads...
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        return factory;
    }
}
