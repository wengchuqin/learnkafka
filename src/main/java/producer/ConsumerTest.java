package producer;

import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest {
    //bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    String zkConnect = "localhost:2181";
    int sessionTimeout = 30000;
    int connectTime = 30000;
    ZkUtils zkUtils = ZkUtils.apply(zkConnect, sessionTimeout, connectTime, JaasUtils.isZkSecurityEnabled());

    @After
    public void after(){
        zkUtils.close();
    }
    @Test
    public void test(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //注意group-id要正确，可以参考consumer.properties
        props.put("group.id", "test-consumer-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
