package com.shl.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 消费者分析
 *
 * @author songhengliang
 * @date 2020/4/18
 */
@Slf4j
public class ConsumerAnalysis {

  // Kafka集群地址
  private static final String brokerList = "localhost:9092";

  // 主题名称-之前已经创建
  private static final String topic = "topictest";

  public static final String groupId = "group.demo";

  public static final AtomicBoolean isRunning = new AtomicBoolean(true);

  public static Properties initConfig() {
    Properties props = new Properties();
    // 与KafkaProducer中设置保持一致
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    // 必填参数，该参数和KafkaProducer中的相同，制定连接Kafka集群所需的broker地址清单，可以设置一个或者多个
    props.put("bootstrap.servers", brokerList);

    // 消费者隶属于的消费组，默认为空，如果设置为空，则会抛出异常，这个参数要设置成具有一定业务含义的名称
    props.put("group.id", groupId);

    // 指定KafkaConsumer对应的客户端ID，默认为空，如果不设置KafkaConsumer会自动生成一个非空字符串
    props.put("client.id", "consumer.client.id.demo");

    return props;
  }

  public static void main(String[] args) {
    Properties props = initConfig();
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topic));

    // 正则订阅主题
    //consumer.subscribe(Pattern.compile("heima*"));

    // 指定订阅的分区
    //consumer.assign(Arrays.asList(new TopicPartition("heima", 0)));

    try {
      while (isRunning.get()) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(
              "topic = " + record.topic() + ", partition = " + record.partition() + ", offset = "
                  + record.offset());
          System.out.println("key = " + record.key() + ", value = " + record.value());
          //do something to process record.
        }
      }
    } catch (Exception e) {
      log.error("occur exception ", e);
    } finally {
      consumer.close();
    }
  }
}
