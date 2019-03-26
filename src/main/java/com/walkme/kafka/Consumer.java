package com.walkme.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Collections;
import java.util.Iterator;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.json.*;
import java.util.Queue; 

public class Consumer {

    private final Queue<Object> mQueue;     // a concurrent queue shared with Node
    private Properties consumProperties;
    private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
    
    public Consumer(Queue<Object> queue, String config){
      mQueue = queue;
      extractPropertiesFromJson(config);
    }

    public void start() {
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(consumProperties, latch, mQueue);
        Thread thread = new Thread(consumerRunnable);
        thread.start();
    
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mLogger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            await(latch);

            mLogger.info("Application has exited");
        }));
    
//        await(latch); // So that the thread doesn't close?
    }

    private void await(CountDownLatch latch) {
        try {
          latch.await();
        } catch (InterruptedException e) {
          mLogger.error("Application got interrupted", e);
        } finally {
          mLogger.info("Application is closing");
        }
      }
    
    private void extractPropertiesFromJson(String jsonString) {
        consumProperties = new Properties();
        JSONObject jsonObject = new JSONObject(jsonString.trim());
        Iterator<String> keys = jsonObject.keys();
        while(keys.hasNext()) {
            String key = keys.next();
            consumProperties.setProperty(key, (String)jsonObject.get(key));
        }
        String deserializer = StringDeserializer.class.getName();
        consumProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        consumProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
    }

    private class ConsumerRunnable implements Runnable {

        private KafkaConsumer<String, String> mConsumer;
        private CountDownLatch mLatch;
        private Queue mQueue;

        ConsumerRunnable(Properties config, CountDownLatch latch, Queue queue) {
            mLatch = latch;
            mQueue = queue;
            String topic = (String)config.get("topic");
            // Handle topic not set here
            config.remove("topic");
            mConsumer = new KafkaConsumer<>(config);
            mConsumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
          try {
            while (true) {
              ConsumerRecords<String, String> records = mConsumer.poll(Duration.ofMillis(100));
    
              for (ConsumerRecord<String, String> record : records) {
                mLogger.info("Key: " + record.key() + ", Value: " + record.value());
                mLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                mQueue.offer(record);
              }
            }
          } catch (WakeupException e) {
            mLogger.info("Received shutdown signal!");
          } finally {
            mConsumer.close();
            mLatch.countDown();
          }
        }

        public void shutdown() {
            mConsumer.wakeup();
        }
    }
}