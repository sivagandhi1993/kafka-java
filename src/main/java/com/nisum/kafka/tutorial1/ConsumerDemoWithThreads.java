package com.nisum.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        ConsumerDemoWithThreads.run();
    }

    private ConsumerDemoWithThreads() {

    }

    private static void run() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //Creating the consumer runnable
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        logger.info("Creating the consumer thread");
        Runnable myConsumerThread = new ConsumerThread(bootstrapServers, groupId, topic, latch);

        //start the thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application is interrupted ", e);
            }
            logger.info("Application has exited");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is interrupted ", e);
        } finally {
            logger.info("Application is closing");
        }
    }
}
