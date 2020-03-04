package com.bk.learnkafka.controller;

import com.bk.learnkafka.domain.LibraryEvent;
import com.bk.learnkafka.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent-sync")
    public ResponseEntity<LibraryEvent> postLibraryEventSynchronous(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

        //invoke kafka producer
        log.info("before sendLibraryEventSynchronous");
        //libraryEventProducer.sendLibraryEvent(libraryEvent);
        SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        log.info("SendResult is {} ", sendResult.toString());
        log.info("after sendLibraryEventSynchronous");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }



    @PostMapping("/v1/libraryevent-sync-to-topic")
    public ResponseEntity<LibraryEvent> postLibraryEventSynchronousToTopic(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

        //invoke kafka producer
        log.info("before sendLibraryEventSynchronousToTopic");
        //libraryEventProducer.sendLibraryEvent(libraryEvent);
        SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronousToTopic(libraryEvent);
        log.info("SendResult is {} ", sendResult.toString());
        log.info("after sendLibraryEventSynchronousToTopic");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        //invoke kafka producer
        log.info("before sendLibraryEvent");
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        log.info("after sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


    @PostMapping("/v1/libraryevent-to-topic")
    public ResponseEntity<LibraryEvent> postLibraryEventToTopic(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        //invoke kafka producer
        log.info("before sendLibraryEventToTopic");
        libraryEventProducer.sendLibraryEventToTopic(libraryEvent);
        log.info("after sendLibraryEventToTopic");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
