package org.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.learnkafka.domain.LibraryEvent;
import org.learnkafka.producer.LibraryEventsProducer;
import org.learnkafka.util.TestUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// TEST SLICE - web layer or entry point layer
// @WebMvcTest - slice the application context, just use the part of application context
@WebMvcTest(LibraryEventsController.class)
public class LibraryEventsControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {
        // given
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class)))
                .thenReturn(new CompletableFuture<>());

        // when
        mockMvc.perform(MockMvcRequestBuilders.post("/api/v1/libraryEvent/approach3")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

        // then
    }

    @Test
    void postLibraryEvent_InvalidValues() throws Exception {
        // given
        var json = objectMapper.writeValueAsString(TestUtil.bookRecordWithInvalidValues());

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class)))
                .thenReturn(new CompletableFuture<>());

        // when
        mockMvc.perform(MockMvcRequestBuilders.post("/api/v1/libraryEvent/approach3")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());

        // then
    }
}
