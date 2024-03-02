package br.com.kualit.springbootkafka.controller;

import br.com.kualit.springbootkafka.domain.LibraryEvent;
import br.com.kualit.springbootkafka.service.LibraryEventsProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    private LibraryEventsProducer producer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) {

        log.info("libraryEvent : {}", libraryEvent);
        producer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

}
