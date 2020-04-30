package com.shl.kafka.monitor;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * 通过代码来获取监控指标
 * JMX Connection
 *
 * @author songhengliang
 * @date 2020/4/12
 */
public class JmxConnectionDemo {

  private MBeanServerConnection conn;
  private String jmxURL;
  private String ipAndPort;

  public JmxConnectionDemo(String ipAndPort) {
    this.ipAndPort = ipAndPort;
  }

  public boolean init() {
    jmxURL = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
    try {
      JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
      JMXConnector connector = JMXConnectorFactory
          .connect(serviceURL, null);
      conn = connector.getMBeanServerConnection();
      if (conn == null) {
        return false;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }


  public double getMsgInPerSec() {
    String objectName = "kafka.server:type=BrokerTopicMetrics," +
        "name=MessagesInPerSec";
    Object val = getAttribute(objectName, "OneMinuteRate");
    if (val != null) {
      return (double) (Double) val;
    }
    return 0.0;
  }

  private Object getAttribute(String objName, String objAttr) {
    ObjectName objectName;
    try {
      objectName = new ObjectName(objName);
      return conn.getAttribute(objectName, objAttr);
    } catch (MalformedObjectNameException | IOException |
        ReflectionException | InstanceNotFoundException |
        AttributeNotFoundException | MBeanException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String[] args) {
    JmxConnectionDemo jmxConnectionDemo =
        new JmxConnectionDemo("localhost:9999");
    jmxConnectionDemo.init();
    System.out.println(jmxConnectionDemo.getMsgInPerSec());
  }
}