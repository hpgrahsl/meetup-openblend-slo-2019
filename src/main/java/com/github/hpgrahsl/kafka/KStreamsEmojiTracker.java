package com.github.hpgrahsl.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class KStreamsEmojiTracker {

    public static void main(String[] args) {
        SpringApplication.run(KStreamsEmojiTracker.class);
    }

    @Component
    public static class KafkaStreamsBootstrap implements CommandLineRunner {

        @Autowired
        KafkaStreams kafkaStreams;

        @Override
        public void run(String... args) {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        }
    }

}
