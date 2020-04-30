package com.shl.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 指定位移消费：增加判断是否分配到了分区
 */
public class SeekDemoAssignment {

  public static final String groupId = "group.demo";
  // Kafka集群地址
  private static final String brokerList = "localhost:9092";
  // 主题名称-之前已经创建
  private static final String topic = "topictest";
  private static AtomicBoolean running = new AtomicBoolean(true);

  public static Properties initConfig() {
    Properties props = new Properties();

    // key发序列化器
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // value反序列化器
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // Kafka集群地址列表
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

    // 消费组
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    // Kafka消费者找不到消费的位移时，从什么位置开始消费，默认：latest 末尾开始消费   earliest：从头开始
    // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    // 是否启用自动位移提交
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    return props;
  }

  public static void main(String[] args) {
    Properties props = initConfig();
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topic));
    long start = System.currentTimeMillis();
    Set<TopicPartition> assignment = new HashSet<>();

    //增加判断是否分配到了分区
    while (assignment.size() == 0) {
      consumer.poll(Duration.ofMillis(100));
      assignment = consumer.assignment();
    }

    long end = System.currentTimeMillis();
    System.out.println(end - start);
    System.out.println(assignment);
    for (TopicPartition tp : assignment) {
      consumer.seek(tp, 10);
    }
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      //consume the record.
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.offset() + ":" + record.value());
      }
    }
  }
}