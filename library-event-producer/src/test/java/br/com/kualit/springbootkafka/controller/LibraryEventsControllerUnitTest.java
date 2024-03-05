package br.com.kualit.springbootkafka.controller;

import br.com.kualit.springbootkafka.domain.LibraryEvent;
import br.com.kualit.springbootkafka.service.LibraryEventsProducer;
import br.com.kualit.springbootkafka.util.TestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


//Slice context
@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc; //This annotation injects the endpoints from the controller.

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void postLibraryEvent() throws Exception {
        var body = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

        when(libraryEventsProducer.sendAsyncLibraryEventWithProducerRecord(isA(LibraryEvent.class)))
                .thenReturn(null);

        mockMvc
                .perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .content(body)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent_invalidValues() throws Exception {
        var body = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());

        when(libraryEventsProducer.sendAsyncLibraryEventWithProducerRecord(isA(LibraryEvent.class)))
                .thenReturn(null);

        var expectedErrorMessage = "errorInfo book.bookId - must not be null, book.bookName - must not be blank";

        mockMvc
                .perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .content(body)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));
    }
}