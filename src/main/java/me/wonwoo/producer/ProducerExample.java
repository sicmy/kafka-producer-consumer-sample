package me.wonwoo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by wonwoo on 2016. 5. 24..
 */
public class ProducerExample {

  public static void main(String[] args) throws IOException {

    Properties configs = new Properties();
    configs.put("bootstrap.servers", "localhost:9092");
    configs.put("acks", "all");
    configs.put("block.on.buffer.full", "true");
    configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

    producer.send(new ProducerRecord<>("test", "hello world"),
      (metadata, exception) -> {
        if (metadata != null) {
          System.out.println(
            "partition(" + metadata.partition() + "), offset(" + metadata.offset() + ")");
        } else {
          exception.printStackTrace();
        }
      });
    producer.flush();
    producer.close();
  }
}
