package com.github.hpgrahsl.kafka.config;

import com.github.hpgrahsl.kafka.emoji.EmojiUtils;
import com.github.hpgrahsl.kafka.model.EmojiCount;
import com.github.hpgrahsl.kafka.model.TopEmojis;
import com.github.hpgrahsl.kafka.model.Tweet;
import com.github.hpgrahsl.kafka.serde.TopNSerdeEC;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashSet;
import java.util.Properties;

@Configuration
public class KafkaStreamsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsConfig.class);

    @Bean
    public Topology kStreamsTopology(KafkaProperties kProps) {

        Serde<String> stringSerde = Serdes.String();
        Serde<EmojiCount> countSerde = new JsonSerde<>(EmojiCount.class);
        Serde<TopEmojis> topNSerde = new TopNSerdeEC(
                Integer.valueOf(kProps.getStreams().getProperties().get("emoji-count-top-n"))
        );

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Tweet> tweets = builder.stream(kProps.getStreams().getProperties().get("tweets-topic"),
                Consumed.with(stringSerde, new JsonSerde<>(Tweet.class)));

        //STATEFUL COUNTING OF ALL EMOJIS CONTAINED IN THE TWEETS
        KTable<String, Long> emojiCounts = tweets
                .map((id,tweet) -> KeyValue.pair(id,EmojiUtils.extractEmojisAsString(tweet.Text)))
                .flatMapValues(emojis -> emojis)
                .map((id,emoji) -> KeyValue.pair(emoji,""))
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .count(Materialized.as(stateStoreSupplier(kProps.getStreams().getProperties().get("state-store-persistence-type"),
                        kProps.getStreams().getProperties().get("state-store-emoji-counts"))));

        emojiCounts.toStream().foreach((emoji,count) -> LOGGER.debug(emoji + "  "+count));

        //MAINTAIN OVERALL TOP N EMOJI SEEN SO FAR
        KTable<String, TopEmojis> mostFrequent = emojiCounts.toStream()
                .map((e, cnt) -> KeyValue.pair("topN", new EmojiCount(e, cnt)))
                .groupByKey(Grouped.with(stringSerde, countSerde))
                .aggregate(
                        () -> new TopEmojis(
                                Integer.valueOf(kProps.getStreams().getProperties().get("emoji-count-top-n"))
                        ),
                        (key, emojiCount, topEmojis) -> {
                            topEmojis.add(emojiCount);
                            return topEmojis;
                        },
                        Materialized.
                                as(stateStoreSupplier(kProps.getStreams().getProperties().get("state-store-persistence-type"),
                                        kProps.getStreams().getProperties().get("state-store-emojis-top-n")))
                                .withValueSerde((Serde)topNSerde)
                );

        //FOR EACH UNIQUE EMOJI WITHIN A TWEET, EMIT & STORE THE TWEET ONCE
        tweets.map((id,tweet) -> KeyValue.pair(tweet.Text,new LinkedHashSet<>(EmojiUtils.extractEmojisAsString(tweet.Text))))
                .flatMapValues(uniqueEmojis -> uniqueEmojis)
                .map((text,emoji) -> KeyValue.pair(emoji, text))
                .to(kProps.getStreams().getProperties().get("emoji-tweets-topic"), Produced.with(stringSerde,stringSerde));

        return builder.build();

    }

    @Bean
    public KafkaStreams kafkaStreams(KafkaProperties kProps, Topology topology) {
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kProps.getConsumer().getAutoOffsetReset());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kProps.getStreams().getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kProps.getStreams().getBootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, kProps.getStreams().getStateDir());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, kProps.getStreams().getCacheMaxBytesBuffering());

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, kProps.getStreams().getProperties().get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, kProps.getStreams().getProperties().get(StreamsConfig.APPLICATION_SERVER_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, kProps.getStreams().getProperties().get(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,kProps.getStreams().getProperties().get(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,kProps.getStreams().getProperties().get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        return new KafkaStreams(topology, props);
    }

    private KeyValueBytesStoreSupplier stateStoreSupplier(String type, String storeName) {

        switch(type) {
            case "in-memory":
                return Stores.inMemoryKeyValueStore(storeName);
            case "rocksdb":
                return Stores.persistentKeyValueStore(storeName);
        }

        throw new IllegalArgumentException("state store persistence type must be either 'in-memory' or 'rocksdb'");
    }

}
