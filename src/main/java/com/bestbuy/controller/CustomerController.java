package com.bestbuy.controller;

import com.bestbuy.kafka.ProductProducer;
import com.bestbuy.model.Product;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/api/customers")
@Slf4j
public class CustomerController {

    @Autowired
    private RestTemplate restTemplate;
    @Value("${product.service.url}")
    private String productServiceUrl;

    @Autowired
    private ProductProducer productProducer;
    private static final String CUSTOMER_SERVICE = "customer_service";
    @GetMapping
    @CircuitBreaker(name = CUSTOMER_SERVICE, fallbackMethod = "getProductListFallback")
    ResponseEntity<List<Product>> getProductList() throws Exception {
        ParameterizedTypeReference<List<Product>> typeRef = new ParameterizedTypeReference<List<Product>>() {};
        try {
            ResponseEntity<List<Product>> response = restTemplate.exchange(productServiceUrl, HttpMethod.GET, null, typeRef);
            log.info("Products retrieved successfully.");
            return response;
        } catch (HttpClientErrorException e) {
            log.error("Failed to retrieve products: {}", e.getMessage());
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    ResponseEntity<List<Product>> getProductListFallback(Exception e) throws Exception {
       return ResponseEntity.ok(Collections.emptyList());
    }

    @PostMapping("/publishProduct")
    public ResponseEntity<String> publishProduct(@RequestBody Product product) throws Exception {
        try {
            productProducer.publishProduct(product);
            return new ResponseEntity<>("Product bought successfully", HttpStatus.OK);
        } catch (Exception e) {
            log.error("Failed to publish product: {}", e.getMessage());
            throw new Exception("Failed to publish products: " + e.getMessage());
        }
    }
    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message) {
        productProducer.sendMessage(message);
        return new ResponseEntity<>("Message sent successfully", HttpStatus.OK);
    }
}
