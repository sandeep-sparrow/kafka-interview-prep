package org.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.learnkafka.Entity.LibraryEvent;
import org.learnkafka.jpa.LibraryEventsRepository;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class LibraryEventService {

    private ObjectMapper objectMapper;
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent: {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                // SAVE OPERATION
                save(libraryEvent);
                break;
            case UPDATE:
                // UPDATE OPERATION
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully Persisted the libraryEvent {} ", libraryEvent);
    }
}
