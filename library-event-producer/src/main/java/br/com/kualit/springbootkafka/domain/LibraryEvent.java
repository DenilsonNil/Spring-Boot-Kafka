package br.com.kualit.springbootkafka.domain;


public record LibraryEvent(Integer libraryEventId, LibraryEventType libraryEventType, Book book) {
}
