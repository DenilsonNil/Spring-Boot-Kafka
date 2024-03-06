package br.com.kualit.springbootkafka.controller;

import br.com.kualit.springbootkafka.domain.LibraryEvent;
import br.com.kualit.springbootkafka.domain.LibraryEventType;
import br.com.kualit.springbootkafka.service.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    private LibraryEventsProducer producer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {

        log.info("libraryEvent : {}", libraryEvent);
        producer.sendAsyncLibraryEventWithProducerRecord(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
        log.info("libraryEvent : {}", libraryEvent);
        if(Objects.isNull(libraryEvent.libraryEventId()))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("please pass the LibraryEventId");

        if(!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE))
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");


        producer.sendAsyncLibraryEventWithProducerRecord(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

}
