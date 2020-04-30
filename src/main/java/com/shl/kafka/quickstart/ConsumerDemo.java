package com.shl.kafka.quickstart;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author songhengliang
 * @date 2020/4/12
 */
public class ConsumerDemo {
  // Kafka集群地址
  private static final String brokerList = "127.0.0.1:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  // 消费组
  private static final String groupId = "group.demo";

  public static void main(String[] args) {
    Properties properties = new Properties();
//    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//    properties.put("bootstrap.servers", brokerList);
//    properties.put("group.id", groupId);

    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));

    while (true) {
      ConsumerRecords<String, String> records =
          consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.value());
      }
    }
  }
}
