package com.github.fernaspiazu.bankbalance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class BankBalanceProducer {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        // kafka bootstrap server
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer!
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
            Stream.generate(customerTransactionSupplier())
                .flatMap(Function.identity())
                .forEach(customer -> {
                    String key = customer.getName();
                    String value = customerToJson(customer);
                    System.out.println("Producing transaction: " + value);
                    ProducerRecord<String, String> record = new ProducerRecord<>("bank-transactions", key, value);
                    kafkaProducer.send(record);
                });
        }
    }

    private static Supplier<Stream<Customer>> customerTransactionSupplier() {
        List<String> customerNames = Collections.unmodifiableList(
            Arrays.asList("John", "Mario", "Alice", "Salvini", "Pablo Escobar", "Gigi")
        );
        return () -> {
            try {
                Thread.sleep(1000);
                return Stream.generate(() -> ThreadLocalRandom.current().nextInt(900) + 100)
                    .limit(100)
                    .map(amount -> {
                        String customerName = customerNames.stream()
                            .skip(ThreadLocalRandom.current().nextInt(customerNames.size() - 1))
                            .findFirst()
                            .orElse("Unknown customer");
                        String time = Instant.now().toString();
                        return new Customer(customerName, amount, time);
                    });
            } catch (InterruptedException e) {
                throw new RuntimeException("1 second sleeping thread was interrupted", e);
            }
        };
    }

    private static String customerToJson(Customer customer) {
        try {
            return mapper.writeValueAsString(customer);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "booom";
        }
    }

}
