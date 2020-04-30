package com.shl.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 自定义分区器test
 *
 * @author songhengliang
 * @date 2020/4/18
 */
public class CustomPartitionerTest {
  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public void test() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");

    // 自定义分区器的使用
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,  CustomPartitioner.class.getName());


    //KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(),
        new StringSerializer());

    // ProducerRecord:指定topic、key、value
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello, Kafka!");

    // 发送消息
    producer.send(record);
  }

}
