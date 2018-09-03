package com.vigneshraja.sessionization.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vigneshraja.sessionization.models.ClickstreamEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by vraja on 9/2/18
 */
public class GenerateEvents {

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 20; i++) {
            ClickstreamEvent event = new ClickstreamEvent("userId" + i, ClickstreamEvent.EventType.PAGE_VIEW);
            producer.send(new ProducerRecord<>("raw-events", Integer.toString(i), mapper.writeValueAsString(event)));
        }

        producer.close();
    }
}
