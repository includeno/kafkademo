package com.example.demo.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class KafkaAdminUtil {

//
//    private AdminClient adminClient;
//
//
//    /**
//     * 创建主题
//     */
//    public void testCreateTopic() throws InterruptedException {
//        NewTopic topic = new NewTopic("topic.quick.simagangl", 3, (short) 1);
//        adminClient.createTopics(Arrays.asList(topic));
//        Thread.sleep(1000);
//    }
//
//    /**
//     * 查看主题
//     */
//    public void testSelectTopicInfo() throws ExecutionException, InterruptedException {
//        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList("topic.quick.simagangl"));
//        result.all().get().forEach((k, v) -> System.out.println("k: " + k + " ,v: " + v.toString() + "\n"));
//    }
//
//    /**
//     * 删除主题
//     */
//    public void deleteTopic() throws InterruptedException {
//        adminClient.deleteTopics(Arrays.asList("topic.quick.simagangl"));
//        Thread.sleep(1000);
//    }
//
//    /**
//     * topic配置描述
//     */
//    public void describeConfig() throws ExecutionException, InterruptedException {
//        DescribeConfigsResult ret = adminClient.describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, "batch_topic")));
//        Map<ConfigResource, Config> configs = ret.all().get();
//        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
//            ConfigResource key = entry.getKey();
//            Config value = entry.getValue();
//            System.out.println(String.format("Resource type: %s, resource name: %s", key.type(), key.name()));
//            Collection<ConfigEntry> configEntries = value.entries();
//            for (ConfigEntry each : configEntries) {
//                System.out.println(each.name() + " = " + each.value());
//            }
//        }
//    }
//
//    /**
//     * 集群描述
//     *
//     * @throws ExecutionException
//     * @throws InterruptedException
//     */
//    public void describeCluster() throws ExecutionException, InterruptedException {
//        DescribeClusterResult ret = adminClient.describeCluster();
//        System.out.println(String.format("Cluster id: %s, controller: %s", ret.clusterId().get(), ret.controller().get()));
//        System.out.println("Current cluster nodes info: ");
//        for (Node node : ret.nodes().get()) {
//            System.out.println(node);
//        }
//
//        Thread.sleep(1000);
//    }
//
//    /**
//     * 更新topic配置
//     */
//    public void alterConfigs() throws ExecutionException, InterruptedException {
//        Config topicConfig = new Config(Arrays.asList(new ConfigEntry("cleanup.policy", "compact")));
//        adminClient.alterConfigs(Collections.singletonMap(
//                new ConfigResource(ConfigResource.Type.TOPIC, "batch_topic"), topicConfig)).all().get();
//    }
//
//    /**
//     * 删除指定主题
//     */
//    public void deleteTopics() throws ExecutionException, InterruptedException {
//        KafkaFuture<Void> futures = adminClient.deleteTopics(Arrays.asList("batch_topic")).all();
//        futures.get();
//    }
//
//    /**
//     * 描述给定的主题
//     */
//    public void describeTopics() throws ExecutionException, InterruptedException {
//        DescribeTopicsResult ret = adminClient.describeTopics(Arrays.asList("batch_topic", "__consumer_offsets"));
//        Map<String, TopicDescription> topics = ret.all().get();
//        for (Map.Entry<String, TopicDescription> entry : topics.entrySet()) {
//            System.out.println(entry.getKey() + " ===> " + entry.getValue());
//        }
//    }
//
//    /**
//     * 打印集群中的所有主题
//     */
//    public void listAllTopics() throws ExecutionException, InterruptedException {
//        ListTopicsOptions options = new ListTopicsOptions();
//        // 包括内部主题，如_consumer_offsets
//        options.listInternal(true);
//        ListTopicsResult topics = adminClient.listTopics(options);
//        Set<String> topicNames = topics.names().get();
//        System.out.println("Current topics in this cluster: " + topicNames);
//    }
}

