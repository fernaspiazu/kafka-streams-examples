package com.github.fernaspiazu.bankbalance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceExactlyOnceApp {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // disable the cache to demonstrate all the "steps" involved in the transformation
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // string Serde
        Serde<String> stringSerde = Serdes.String();

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> bankTransactionStream = builder.stream("bank-transactions", Consumed.with(stringSerde, jsonSerde));

        KTable<String, JsonNode> totalBalance = bankTransactionStream
            .groupByKey()
            .aggregate(
                BankBalanceExactlyOnceApp::initialBalance,
                (key, transaction, balance) -> newBalance(transaction, balance),
                Materialized.<String, JsonNode>as(Stores.persistentKeyValueStore("balance-aggregation"))
                    .withKeySerde(stringSerde)
                    .withValueSerde(jsonSerde)
            );

        totalBalance.toStream().to("bank-balance-exactly-once", Produced.with(stringSerde, jsonSerde));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode initialBalance() {
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());
        return initialBalance;
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        String newBalanceInstant = Instant.ofEpochMilli(Math.max(transactionEpoch, balanceEpoch)).toString();

        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
        newBalance.put("time", newBalanceInstant);
        return newBalance;
    }
}
