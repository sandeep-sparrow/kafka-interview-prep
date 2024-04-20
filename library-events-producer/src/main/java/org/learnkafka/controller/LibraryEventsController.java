package org.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.learnkafka.domain.LibraryEvent;
import org.learnkafka.domain.LibraryEventType;
import org.learnkafka.producer.LibraryEventsProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api")
@Slf4j
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController(LibraryEventsProducer libraryEventsProducer){
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/libraryEvent/approach1")
    public ResponseEntity<LibraryEvent> postLibraryEvent1(
            @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("libraryEvent: {}", libraryEvent);
        // invoke the kafka producer
        libraryEventsProducer.sendLibraryEvent_approach1(libraryEvent);

        log.info("After sending");
        // return data container
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/v1/libraryEvent/approach2")
    public ResponseEntity<LibraryEvent> postLibraryEvent2(
            @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        log.info("libraryEvent: {}", libraryEvent);
        // invoke the kafka producer
        libraryEventsProducer.sendLibraryEvent_approach2(libraryEvent);

        log.info("After sending");
        // return data container
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/v1/libraryEvent/approach3")
    public ResponseEntity<LibraryEvent> postLibraryEvent3(
            @RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("libraryEvent: {}", libraryEvent);
        // invoke the kafka producer
        CompletableFuture<SendResult<Integer, String>> result = libraryEventsProducer.sendLibraryEvent_approach3(libraryEvent);

        log.info(result.toString());

        log.info("After sending");
        // return data container
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryEvent")
    public ResponseEntity<?> updateLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("libraryEvent: {}", libraryEvent);
        // invoke the kafka producer
        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        libraryEventsProducer.sendLibraryEvent_approach3(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if(libraryEvent.libraryEventId() == null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if(!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE)){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only Update event type is supported");
        }
        return null;
    }
}
