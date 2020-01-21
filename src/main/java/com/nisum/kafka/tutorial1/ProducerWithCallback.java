package com.nisum.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerWithCallback {

    private static final String bootstarpServers = "127.0.0.1:9092";

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ProducerWithCallback.class);
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarpServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //create producer record
        for(int i = 0 ; i <= 10 ; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world " + i);
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //excuetes every time  a record  is successfully  sent or an exception is thrown
                    if (Objects.isNull(exception)) {
                        // if the record is successfully sent
                        log.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n");
                    } else {
                        log.error("Error while producing: " + exception);
                    }
                }
            });
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
