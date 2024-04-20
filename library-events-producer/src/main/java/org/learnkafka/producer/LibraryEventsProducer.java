package org.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.learnkafka.domain.LibraryEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    public String producerTopic;

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper){
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    // asynchronously
    public void sendLibraryEvent_approach1(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var completableFuture = kafkaTemplate.send(producerTopic, key, value);

        completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, throwable);
                    } else {
                        handleSuccess(key, value, sendResult);
                    }
                });
    }

    // synchronously
    public void sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        // block and wait until the message is sent to kafka
        var sendResult = kafkaTemplate.send(producerTopic, key, value).get();
        handleSuccess(key, value, sendResult);
    }

    // asynchronously - send message using ProducerRecord
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var producerRecord = buildProducerRecord(key, value);

        var completableFuture = kafkaTemplate.send(producerRecord);

        return completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, throwable);
                    } else {
                        handleSuccess(key, value, sendResult);
                    }
                });
    }

    public ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value){
        return new ProducerRecord<>(producerTopic, key, value);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message send successfully for the key: {} and the value: {}, partition is {} ",
                key, value, sendResult.getRecordMetadata().partition()
        );
    }

    private void handleFailure(Integer key, Throwable throwable) {
        log.error("Error Sending the message and the exception is {}", throwable.getMessage(), throwable);
    }


}
