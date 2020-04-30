package com.shl.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

/**
 * 生产者分析
 * @author songhengliang
 * @date 2020/4/18
 */
public class ProducerAnalysis {

  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public static Properties initConfig() {
    Properties props = new Properties();

    // 指定brokers地址清单，格式host:port，清单里不需要包含所有的broker地址，
    // 生产者会从给定的broker里查找到其它broker的信息，建议至少提供两个broker的信息，因为一旦其中一个宕机，生产者仍然能够连接到集群上
    props.put("bootstrap.servers", brokerList);

    // 默认提供了StringSerializer、IntegerSerializer、ByteArraySerializer，也可以自定义序列化器
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    // 设定Producer对应的客户端id，默认为空，如果不设置Kafka会自动生成一个非空字符串
    // 内容形式如："producer-1"
    props.put("client.id", "producer.client.id.demo");

    return props;
  }

  @Test
  public void test1() {
    Properties props = initConfig();

    //KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(),
        new StringSerializer());

    // ProducerRecord:指定topic、key、value
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello, Kafka!");

    // 发送消息
    producer.send(record);

    // 同步发送
    // 如果kafka正常响应，返回一个RecordMetadata对象，该对象存储消息的偏移量
    // 如果kafka发生错误，无法正常响应，就会抛出异常，我们便可以进行异常处理
    try {
      Future<RecordMetadata> future = producer.send(record);
      RecordMetadata recordMetadata = future.get();
      System.out.println("part:" + recordMetadata.partition() + ";topic:" + recordMetadata.topic());
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 异步发送
    producer.send(record, new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
          System.out.println(metadata.partition() + ":" + metadata.offset());
        }
      }
    });
  }
}
