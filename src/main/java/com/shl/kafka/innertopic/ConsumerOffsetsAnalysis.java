package com.shl.kafka.innertopic;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

/**
 * kafka内部topic：_consumer_offsets是一个内部topic
 * @author songhengliang
 * @date 2020/4/12
 */
public class ConsumerOffsetsAnalysis {
    // Kafka集群地址
    private static final String brokerList = "127.0.0.1:9092";
    // 主题名称-之前已经创建
    private static final String topic = "topictest";
    // 消费组
    private static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "latest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);

        Consumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(properties);
        consumer.subscribe(Collections.singletonList("__consumer_offsets"));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
            Map<String, Integer> map = new HashMap<>();
            while (iterator.hasNext()) {
                ConsumerRecord<byte[], byte[]> record = iterator.next();
                if (record.key() == null) {
                    continue;
                }
                System.out.println("topic:" + record.topic() + ",partition:" + record.partition() + ",offset:" + record.offset());
            }
        }
    }
}
