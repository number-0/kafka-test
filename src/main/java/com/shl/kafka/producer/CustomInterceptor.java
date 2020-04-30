package com.shl.kafka.producer;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 自定义拦截器
 */
public class CustomInterceptor implements ProducerInterceptor<String, String> {

  private volatile long sendSuccess = 0;
  private volatile long sendFailure = 0;

  /**
   * 拦截
   * @param record
   * @return
   */
  @Override
  public ProducerRecord<String, String> onSend(
      ProducerRecord<String, String> record) {
    //拦截消息，加上前缀prefix1-
    String modifiedValue = "prefix1-" + record.value();
    return new ProducerRecord<>(record.topic(),
        record.partition(), record.timestamp(),
        record.key(), modifiedValue, record.headers());
//        if (record.value().length() < 5) {
//            throw new RuntimeException();
//        }
//        return record;
  }

  /**
   * 接受到ack后操作，发送成功或失败
   * @param recordMetadata
   * @param e
   */
  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    if (e == null) {
      sendSuccess++;
    } else {
      sendFailure++;
    }
  }

  @Override
  public void close() {
    double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
    System.out.println("[INFO] 发送成功率="
        + String.format("%f", successRatio * 100) + "%");
  }

  @Override
  public void configure(Map<String, ?> map) {
  }
}