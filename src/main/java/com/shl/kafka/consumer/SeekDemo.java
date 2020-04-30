package com.shl.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
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
 * 指定位移消费
 */
public class SeekDemo {

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
    // timeout参数设置多少合适？太短会使分区分配失败，太长又有可能造成一些不必要的等待
    consumer.poll(Duration.ofMillis(2000));

    // 获取消费者所分配到的分区
    Set<TopicPartition> assignment = consumer.assignment();
    System.out.println(assignment);

    for (TopicPartition tp : assignment) {
      // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
      consumer.seek(tp, 10);
    }

    //consumer.seek(new TopicPartition(topic,0),10);
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      //consume the record.
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.offset() + ":" + record.value());
      }
    }
  }

}