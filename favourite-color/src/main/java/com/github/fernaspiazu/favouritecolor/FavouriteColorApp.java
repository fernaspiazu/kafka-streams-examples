package com.github.fernaspiazu.favouritecolor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColorApp {

    private static final List<String> ALLOWED_COLOURS = Arrays.asList("green", "red", "blue");

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> favColorInput = builder.stream("favourite-color-input");

        favColorInput
            .filter((key, value) -> value.split(",").length == 2)
            .selectKey((key, value) -> value.split(",")[0])
            .mapValues(value -> value.split(",")[1].toLowerCase())
            .filter((key, value) -> ALLOWED_COLOURS.contains(value))
            .to("favourite-color-map");

        KTable<String, String> favColorTable = builder.table("favourite-color-map");
        favColorTable
            .groupBy((key, value) -> KeyValue.pair(value, 1L), Serialized.with(Serdes.String(), Serdes.Long()))
            .count(Materialized.as("colour-count"))
            .toStream()
            .to("favourite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        KafkaStreams stream = new KafkaStreams(topology, config);
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}
