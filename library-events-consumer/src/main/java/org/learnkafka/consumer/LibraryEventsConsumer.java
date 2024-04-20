package org.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.learnkafka.service.LibraryEventService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class LibraryEventsConsumer {

    private LibraryEventService libraryEventService;

    @KafkaListener(topics = {"library-events"}, groupId = "Library-events-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord : {} ", consumerRecord);
        libraryEventService.processLibraryEvent(consumerRecord);
    }
}
