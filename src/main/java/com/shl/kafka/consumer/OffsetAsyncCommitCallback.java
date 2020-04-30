package com.shl.kafka.consumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 位移提交：异步提交
 */
@Slf4j
public class OffsetAsyncCommitCallback {

  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public static final String groupId = "group.demo";

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

    try {
      while (running.get()) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
          //do some logical processing.
        }
        // 异步回调
        consumer.commitAsync(new OffsetCommitCallback() {
          @Override
          public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
              Exception exception) {
            if (exception == null) {
              System.out.println(offsets);
            } else {
              log.error("fail to commit offsets {}", offsets, exception);
            }
          }
        });
      }
    } finally {
      consumer.close();
    }

    try {
      while (running.get()) {
        consumer.commitAsync();
      }
    } finally {
      try {
        consumer.commitSync();
      } finally {
        consumer.close();
      }
    }
  }
}