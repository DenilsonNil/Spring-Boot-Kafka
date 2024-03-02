package br.com.kualit.springbootkafka.service;

import br.com.kualit.springbootkafka.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    public String topic;
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;


    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent)  {
        var key = libraryEvent.libraryEventId();
        String value;

        try {
            value = objectMapper.writeValueAsString(libraryEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        var completableFuture = kafkaTemplate.send(topic, key, value);
        return processLibraryEvent(key, value, completableFuture);
    }

    private CompletableFuture<SendResult<Integer, String>> processLibraryEvent(Integer key, String value, CompletableFuture<SendResult<Integer, String>> completableFuture) {
        return completableFuture
            .whenComplete((sendResult, throwable) -> {
                if (throwable != null)
                    handleFailure(key, value, throwable);
                else
                    handleSuccess(key, value, sendResult);
            });
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message and the exception is {}", ex.getMessage(), ex);
    }
    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent sucessfully for the key {}, and the value {}, partition is {}",
                key, value, sendResult.getRecordMetadata().partition());
    }
}
