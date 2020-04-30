package com.shl.kafka.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 位移提交：同步提交
 * @author songhengliang
 * @date 2020/4/18
 */
public class OffsetSyncCommit {
  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public static final String groupId = "group.demo";

  private static AtomicBoolean running = new AtomicBoolean(true);

  public static Properties initConfig() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // 手动提交开启
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return props;
  }

  public static void main(String[] args) {
    Properties props = initConfig();
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


    TopicPartition tp = new TopicPartition(topic, 0);
    consumer.assign(Arrays.asList(tp));
    long lastConsumedOffset = -1;
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      if (records.isEmpty()) {
        break;
      }
      List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
      lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
      consumer.commitSync();//同步提交消费位移
    }
    System.out.println("comsumed offset is " + lastConsumedOffset);
    OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
    System.out.println("commited offset is " + offsetAndMetadata.offset());
    long posititon = consumer.position(tp);
    System.out.println("the offset of the next record is " + posititon);
  }

}
