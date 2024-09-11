/**
 * @(#)CreateTopicDemo.java, 2024/9/11
 * <p/>
 * Copyright 2022 fenbi.com. All rights reserved.
 * FENBI.COM PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package kafka.examples.local;

import kafka.examples.KafkaProperties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author zwb
 */
public class DelAndCreateTopicDemo {
    // 重建一个 topic？
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 构造 KafkaAdmin 对象
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        Admin adminClient = Admin.create(props);
        // 确定要删除的 topic list
        List<String> needDeleteTopics = Arrays.asList(KafkaProperties.TOPIC);
        // 删除 topic
        try {
            adminClient.deleteTopics(needDeleteTopics).all().get();
        } catch (ExecutionException e) {
            System.out.println("Failed to delete topics: " + needDeleteTopics);
        }

//         校验确实删掉了
        while (true) {
            System.out.println("Making sure the topics are deleted successfully: " + needDeleteTopics);

            Set<String> listedTopics = adminClient.listTopics().names().get();
            System.out.println("Current list of topics: " + listedTopics);

            boolean hasTopicInfo = false;
            for (String listedTopic : listedTopics) {
                if (needDeleteTopics.contains(listedTopic)) {
                    hasTopicInfo = true;
                    break;
                }
            }
            if (!hasTopicInfo) {
                break;
            }
            Thread.sleep(1000);
        }

        List<NewTopic> needCreateTopics = needDeleteTopics.stream()
                .map(topicName -> new NewTopic(topicName, 2, (short) 2))
                .collect(Collectors.toList());
        // 创建 topic
        adminClient.createTopics(needCreateTopics).all().get();
    }
}
