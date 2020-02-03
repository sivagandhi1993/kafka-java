package com.nisum.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String, String> kafkaConsumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

    public ConsumerThread(String bootstrapServers,
                          String groupId,
                          String topic,
                          CountDownLatch latch) {
        this.latch = latch;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        kafkaConsumer = new KafkaConsumer<>(properties);
        //subscribe the consumer to our topic
        kafkaConsumer.subscribe(Collections.singleton(topic));

    }

    @Override
    public void run() {
        //poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                records.forEach(t -> {
                    logger.info("Key: " + t.key() + "Value: " + t.value());
                    logger.info("Partition: " + t.partition() + "Offset: " + t.offset());
                });
            }
        } catch (WakeupException e) {
            logger.info("Recieved shutdown signal!");
        } finally {
            kafkaConsumer.close();
            //tell our main code when we are done with the consumer
            latch.countDown();
        }
    }

    public void shutdown() {
        //the wakeup() method  is a special method to interrupt consumer.poll()
        //It will throw the exception called wakeupException
        kafkaConsumer.wakeup();
    }
}
