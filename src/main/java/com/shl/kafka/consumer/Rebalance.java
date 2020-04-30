package com.shl.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


/**
 * 再均衡监听器
 */
public class Rebalance {

  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public static final String groupId = "group.demo";

  public static final AtomicBoolean isRunning = new AtomicBoolean(true);

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

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // 劲量避免重复消费，因为下面操作为consumer.commitAsync(currentOffsets, null);
        consumer.commitSync(currentOffsets);
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        //do nothing.
      }
    });

    try {
      while (isRunning.get()) {
        ConsumerRecords<String, String> records =
            consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(record.offset() + ":" + record.value());
          // 异步提交消费位移，在发生再均衡动作之前可以通过再均衡监听器的onPartitionsRevoked回调执行commitSync方法同步提交位移。
          currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1));
        }
        consumer.commitAsync(currentOffsets, null);
      }
    } finally {
      consumer.close();
    }

  }
}
