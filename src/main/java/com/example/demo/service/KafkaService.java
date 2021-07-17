package com.example.demo.service;

import com.example.demo.config.KafkaTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class KafkaService {
    @Autowired
    KafkaTemplate kafkaTemplate;

    public ListenableFuture<SendResult<Integer, String>> sendAsync(String message){
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(KafkaTopic.test, message);

        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                log.info(result.getProducerRecord().value());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error(ex.getMessage(),KafkaTopic.test, message);
            }

        });

        return future;
    }

    public Boolean sendSync(String message){
        ProducerRecord<String, String> record =new ProducerRecord<>(KafkaTopic.test,message);

        try {
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
            return Boolean.TRUE;
        }
        catch (ExecutionException e) {
            //handleFailure(data, record, e.getCause());
            log.error(e.getMessage());
            return Boolean.FALSE;
        }
        catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
        finally {

        }
    }
}
