package producer;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Test;

import java.util.Properties;

public class AdminUtilsTest {
    //bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    String zkConnect = "localhost:2181";
    int sessionTimeout = 30000;
    int connectTime = 30000;
    ZkUtils zkUtils = ZkUtils.apply(zkConnect, sessionTimeout, connectTime, JaasUtils.isZkSecurityEnabled());

    @After
    public void after() {
        zkUtils.close();
    }

    @Test
    public void createTopic() {
        String topic = "my-topic2";
        if (!AdminUtils.topicExists(zkUtils, topic)) {
            System.out.printf("topic:%s not exist\n", topic);
            int repilca = 1;
            int patition = 1;
            Properties props = new Properties();
            AdminUtils.createTopic(zkUtils, topic, patition, repilca, props,
                    RackAwareMode.Enforced$.MODULE$);
        }else {
            System.out.printf("topic:%s exist\n", topic);
        }


    }

    @Test
    public void deleteTest() {
        AdminUtils.deleteTopic(zkUtils, "test");
    }


}
