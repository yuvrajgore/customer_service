package com.bestbuy.kafka;

import com.bestbuy.model.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class ProductProducer {
    @Autowired
    private KafkaTemplate<String, Product> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateMessage;

    public void publishProduct(Product product) throws Exception {
        log.info(String.format("Product published %s", product));
        Message<Product> message = MessageBuilder
                .withPayload(product)
                .setHeader(KafkaHeaders.TOPIC, "pocProduct")
                .build();

        CompletableFuture<SendResult<String, Product>> response = kafkaTemplate.send(message);
        log.info("Product published: {}", response.get().toString());
    }

    public void sendMessage(String message){
        log.info(String.format("Message sent %s", message));
        kafkaTemplateMessage.send("pocStringTopic", message);
    }
}
