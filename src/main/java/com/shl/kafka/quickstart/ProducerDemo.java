package com.shl.kafka.quickstart;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author songhengliang
 * @date 2020/4/12
 */
public class ProducerDemo {

  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public static void main(String[] args) {
    Properties properties = new Properties();
    // 设置key序列化器
    //properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // 设置重试次数
    properties.put(ProducerConfig.RETRIES_CONFIG, 10);

    // 设置值序列化器
    //properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // 设置集群地址
    //properties.put("bootstrap.servers", brokerList);
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

    // KafkaProducer 线程安全
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Kafka-demo", "hello, Kafka!");

    try {
      producer.send(record);
      //RecordMetadata recordMetadata = producer.send(record).get();
      //System.out.println("part:" + recordMetadata.partition() + ";topic:" + recordMetadata.topic());
    } catch (Exception e) {
      e.printStackTrace();
    }
    producer.close();
  }
}
