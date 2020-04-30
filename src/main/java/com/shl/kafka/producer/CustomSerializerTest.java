package com.shl.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 自定义序列化器test
 */
public class CustomSerializerTest {
  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public static void main(String[] args)
      throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());
    // properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtostuffSerializer.class.getName());
    properties.put("bootstrap.servers", brokerList);

    KafkaProducer<String, Company> producer =
        new KafkaProducer<>(properties);
    Company company = Company.builder().name("kafka")
        .address("北京").build();
    //Company company = Company.builder().name("hiddenkafka")
        // .address("China").telphone("13000000000").build();
    ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);
    producer.send(record).get();
  }
}