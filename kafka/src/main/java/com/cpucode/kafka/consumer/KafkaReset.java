package com.cpucode.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Kafka消费者  offset重置问题
 *
 * auto.offset.reset = earliest | latest
 *
 * 重置的情况:
 *  1. 新的组 当前的消费者组在kafka中没有消费记录
 *  2. 要消费的offset对应的消息已经被删除
 *
 * @author : cpucode
 * @date : 2021/11/30 19:06
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class KafkaReset {
    public static void main(String[] args) {
        //创建配置对象
        Properties props = new Properties() ;
        // Kafka集群位置
        props.put("bootstrap.servers", "cpucode100:9092");
        // 消费者组id
        props.put("group.id", "cpuCode");

        // 自动提交offset
        props.put("enable.auto.commit", "true");
        // offset提交的间隔
        props.put("auto.commit.interval.ms", "1000");

        // kv的反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 重置offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //1. 创建消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);

        //2. 订阅主题
        List<String> topics = new ArrayList<>();
        topics.add("first");
        topics.add("hello");
        topics.add("second");
        kafkaConsumer.subscribe(topics);

        //3. 持续消费数据
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("消费到: " + record.topic() +
                        " : " + record.partition() +
                        " : " + record.offset() +
                        " : " + record.key() +
                        " : " + record.value());
            }
        }

        // 关闭生产者对象
        //kafkaConsumer.close();
    }
}
