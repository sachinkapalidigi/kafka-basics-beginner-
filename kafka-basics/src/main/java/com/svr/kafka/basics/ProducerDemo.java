package com.svr.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Konichiwa, watashi no Kafka producer desu");

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer <both key value pair are strings>
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Konichiwa, watashi no Kafka producer desu: "+i);
            // goes to same partition - this behaviour is of StickyPartitioner - happens for performance improvements
            // it batches to make it more efficient

            // send data - asynchronous operation
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if(exception == null) {
                        log.info("Received new metadata/ \n"+
                                "Topic: "+metadata.topic()+"\n"+
                                "Partition: "+metadata.partition()+"\n"+
                                "Offset: "+metadata.offset()+"\n"+
                                "Timestamp: "+metadata.timestamp()
                        );
                    }else {
                        log.error("Error while producing", exception);
                    }
                }
            });

            // to pick round robin instead of sticky partitioner - change wait duration to see this behaviour
            try {
                Thread.sleep(2000);
            }catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("--------------------------------When was this logged?---------------------");
        // flush and close the Producer
        // flush - synchronous

        producer.flush();
        producer.close(); // calls flush internally

    }
}
