package org.learnkafka.controller;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.learnkafka.domain.LibraryEvent;
import org.learnkafka.util.TestUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

// @SpringBootTest -> brings the whole spring application context
@SpringBootTest(webEnvironment =  SpringBootTest.WebEnvironment.RANDOM_PORT)
class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Test
    void postLibraryEvent() {
        // given
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(), httpHeaders);

        // when
        var responseEntity = testRestTemplate.exchange("/api/v1/libraryEvent/approach2", HttpMethod.POST,
                httpEntity, LibraryEvent.class);

        // then
        Assertions.assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }
}
