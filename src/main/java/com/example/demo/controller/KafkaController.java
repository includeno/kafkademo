package com.example.demo.controller;

import com.example.demo.config.KafkaTopic;
import com.example.demo.service.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RestController
public class KafkaController {

    @Autowired
    KafkaService kafkaService;

    //(Async)
    @GetMapping("/asyncsend")
    public String send(String message) throws ExecutionException, InterruptedException {

        ListenableFuture<SendResult<Integer, String>> future=kafkaService.sendAsync(message);
        future.get();
        return message+" send";
    }


    //(Async)
    @GetMapping("/syncsend")
    public String syncsend(String message) throws ExecutionException, InterruptedException {

        Boolean result=kafkaService.sendSync(message);
        if(result==Boolean.TRUE){
            return message+" succeed";
        }
        return message+" failed";
    }

}
