package com.walkme.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

class StreamTest {

    private final static String bootstrapServers = "localhost:9092";
    private final static String inputTopic = "full-events";
    private final static String applicationId = "streams-app";

    public static void main(String[] args) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<FullEvent> fullEventSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", FullEvent.class);
        fullEventSerializer.configure(serdeProps, false);

        final Deserializer<FullEvent> fullEventDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", FullEvent.class);
        fullEventDeserializer.configure(serdeProps, false);

        final Serde<FullEvent> fullEventSerde = Serdes.serdeFrom(fullEventSerializer, fullEventDeserializer);

        final Serializer<ClickEvents> clickEventsSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ErrorEvents.class);
        clickEventsSerializer.configure(serdeProps, false);

        final Deserializer<ClickEvents> clickEventsDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ClickEvents.class);
        clickEventsDeserializer.configure(serdeProps, false);

        final Serde<ClickEvents> clickEventsSerde = Serdes.serdeFrom(clickEventsSerializer, clickEventsDeserializer);

        // KStream<String, FullEvent> fullEvents = builder.stream(inputTopic, Consumed.with(Serdes.String(), fullEventSerde));
        // fullEvents
        // .filter((sessionId, event) -> event.headers.action.equals("mouseClick") || event.headers.action.equals("error"))
        // .groupByKey(Serialized.with(Serdes.String(), fullEventSerde))
        // .windowedBy(new)

        KGroupedStream<String, FullEvent> groupedFullEvents = fullEvents.groupByKey(Serialized.with(Serdes.String(), fullEventSerde));

        KTable<String, ClickEvents> aggregatedClickEvents = groupedFullEvents.aggregate(  
            () -> new ClickEvents(),
            (sessionId, newEvent, allErrorEvents) -> {
                if (newEvent.routingKey.equals("mouseClick")) {
                    allErrorEvents.addEvent(newEvent);
                }
                return allErrorEvents; 
            },
            Materialized.as("aggregated-click-events")
                .with(Serdes.String(), clickEventsSerde)
        );


        KStream<String, FullEvent> errorClicksStream = fullEvents.join(aggregatedClickEvents, 
            (fullEvent, clickEvents) -> {
                Optional<FullEvent> prevClickEvent = clickEvents.fullevents
                    .stream()
                    .filter(clickEvent -> clickEvent.headers.sequenceIndex == fullEvent.headers.sequenceIndex - 1)
                    .findFirst(); 

                if (prevClickEvent.isPresent() && fullEvent.headers.action.equals("error")) {
                    fullEvent.isErrorClick = true; 
                }
                return fullEvent;
            },
            Joined.keySerde(Serdes.String()))
            .filter((sessionId, fullEvent) -> fullEvent.isErrorClick != null && fullEvent.isErrorClick);

            errorClicksStream.to("error-clicks", Produced.with(Serdes.String(), fullEventSerde));


        
        KTable<Windowed<String>, Long> liveSessions = groupedFullEvents
            .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(12)).advanceBy(TimeUnit.SECONDS.toMillis(1)))
            .aggregate(
                () -> null,
                (sessionId, newEvent, lastEventTime) -> {
                    return newEvent.headers.sequenceIndex; 
                },
                Materialized.as("live-sessions")
                    .with(Serdes.String(), Serdes.Long())
            );

        // KStream<String, FullEvent> sessionEndedStream = fullEvents.join(liveSessions,
        //     (fullEvent, lastEventTime) -> {
        //         return fullEvent;
        //     },
        //     Joined.keySerde(Serdes.String()));
        

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        ReadOnlyKeyValueStore<String, Long> keyValueStore =
            streams.store("live-sessions", QueryableStoreTypes.keyValueStore());

        streams.start();


    }

}

