package br.com.kualit.springbootkafka.service;

import br.com.kualit.springbootkafka.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

    public CompletableFuture<SendResult<Integer, String>> sendAsyncLibraryEvent(LibraryEvent libraryEvent)  {
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

    public SendResult<Integer, String> sendSyncLibraryEvent(LibraryEvent libraryEvent)  {
        var key = libraryEvent.libraryEventId();
        String value;
        SendResult<Integer, String> sendResult;

        try {
            value = objectMapper.writeValueAsString(libraryEvent);
            sendResult = kafkaTemplate.send(topic, key, value).get();
        } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        handleSuccess(key, value, sendResult);
        return sendResult;
    }


    public CompletableFuture<SendResult<Integer, String>> sendAsyncLibraryEventWithProducerRecord(LibraryEvent libraryEvent)  {
        var key = libraryEvent.libraryEventId();
        String value;

        try {
            value = objectMapper.writeValueAsString(libraryEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        var producerRecord = buildProducerRecord(key, value);
        var completableFuture = kafkaTemplate.send(producerRecord);
        return processLibraryEvent(key, value, completableFuture);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, headers);
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
