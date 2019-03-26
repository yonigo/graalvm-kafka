package com.walkme.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import org.json.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {


    public static void main(String[] args) {
        Producer p = new Producer("{\"bootstrap.servers\": \"localhost:9092\", }");
        try {
            p.put("test_topic", "msgKey", "msgData");
        }
        catch (Exception e) {
            System.out.println("Error Putting" + e);
        }
    }

    private Properties produceProperties;
    private final KafkaProducer<String, String> mProducer;
    private final Logger mLogger = LoggerFactory.getLogger(Producer.class);

    public Producer(String config) {
        extractPropertiesFromJson(config);
        mProducer = new KafkaProducer<>(produceProperties);

        mLogger.info("Producer initialized");
    }

    public void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        mLogger.info("Put value: " + value + ", for key: " + key);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        mProducer.send(record, (recordMetadata, e) -> {
        if (e != null) {
            mLogger.error("Error while producing", e);
            return;
        }

        mLogger.info("Received new meta. Topic: " + recordMetadata.topic()
            + "; Partition: " + recordMetadata.partition()
            + "; Offset: " + recordMetadata.offset()
            + "; Timestamp: " + recordMetadata.timestamp());
        }).get();
    }

    void close() {
        mLogger.info("Closing producer's connection");
        mProducer.close();
    }

    private void extractPropertiesFromJson(String jsonString) {
        produceProperties = new Properties();
        JSONObject jsonObject = new JSONObject(jsonString.trim());
        Iterator<String> keys = jsonObject.keys();
        while(keys.hasNext()) {
            String key = keys.next();
            produceProperties.setProperty(key, (String)jsonObject.get(key));
        }
        String deserializer = StringSerializer.class.getName();
        produceProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, deserializer);
        produceProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, deserializer);
    }
}