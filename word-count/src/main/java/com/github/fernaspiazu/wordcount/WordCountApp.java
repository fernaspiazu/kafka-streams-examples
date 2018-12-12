package com.github.fernaspiazu.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 1 - stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = wordCountInput
            // 2 - map values to lowercase
            .mapValues((ValueMapper<String, String>) String::toLowerCase)
            // 3 - flatmap value split by space
            .flatMapValues(lowercasedTextline -> Arrays.asList(lowercasedTextline.split(" ")))
            // 4 - select key to apply a key (we discard the old key)
            .selectKey((ignoredKey, word) -> word)
            // 5 - group by key before aggregation
            .groupByKey()
            // 6 - count occurrences
            .count(Materialized.as("Counts"));

        // 7 - in order to write back the result to kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();
        streams.start();

        // printed topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the stream application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
