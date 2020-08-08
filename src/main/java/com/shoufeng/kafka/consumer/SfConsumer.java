package com.shoufeng.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * @author shoufeng
 */
@Component
public class SfConsumer {

    @KafkaListener(groupId = "sf_demo_consumer_group", id = "sf_demo_consumer_group_01", topics = {"sf_demo_topic_01"},
            topicPartitions = {@TopicPartition(topic = "sf_demo_topic_01", partitions = {"1"})}, //指定topic的分区
            concurrency = "4" //并发消费
    )
    public void test01(ConsumerRecord<Integer, String> consumerRecord) {
        System.out.println(consumerRecord.value());
        System.out.println("sf_demo_consumer_group_01: " + consumerRecord.toString());
    }

    @KafkaListener(groupId = "sf_demo_consumer_group_second", id = "sf_demo_consumer_group_second_01", topics = {"sf_demo_topic_01"})
    public void test02(ConsumerRecord<Integer, String> consumerRecord) {
        System.out.println("sf_demo_consumer_group_second_01: " + consumerRecord.toString());
    }

    @KafkaListener(groupId = "sf_demo_consumer_group", id = "sf_demo_consumer_group_02", topics = {"sf_demo_topic_01"})
    public void test03(ConsumerRecord<Integer, String> consumerRecord) {
        System.out.println("sf_demo_consumer_group_02: " + consumerRecord.toString());
    }
}
