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
 * @author : cpucode
 * @date : 2021/11/30 19:16
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class ConsumerSync {
    public static void main(String[] args) {
        //创建配置对象
        Properties props = new Properties() ;
        // Kafka集群位置
        props.put("bootstrap.servers", "cpucode100:9092");
        // 消费者组id
        props.put("group.id", "cpuCode");

        //关闭自动提交offset
        props.put("enable.auto.commit", "false");
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
            System.out.println("进行下一次的消费");

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("消费到: " + record.topic() +
                        " : " + record.partition() +
                        " : " + record.offset() +
                        " : " + record.key() +
                        " : " + record.value());
            }

            // 同步提交offset
            System.out.println("同步提交offset");
            kafkaConsumer.commitSync();

            System.out.println();
        }

        // 关闭生产者对象
        //kafkaConsumer.close();
    }
}
