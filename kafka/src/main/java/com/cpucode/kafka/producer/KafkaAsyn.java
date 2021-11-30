package com.cpucode.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * 生产者 - 异步发送 - 不带回调
 *
 * 配置类
 *   CommonClientConfigs : 通用的配置类
 *   ProducerConfig ： 生产者的配置类
 *   ConsumerConfig :  消费者的配置类
 *
 * @author : cpucode
 * @date : 2021/11/30 16:28
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class KafkaAsyn {
    public static void main(String[] args) {
        //创建配置对象
        Properties props = new Properties();

        //kafka集群，broker-list
        props.put("bootstrap.servers", "cpucode100:9092");
        // ack的级别
        props.put("acks", "all");
        //重试次数
        props.put("retries", 3);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator缓冲区大小
        props.put("buffer.memory", 33554432);  //32M

        // kv的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // 生产数据
        for (int i = 0; i < 10; i++) {
            // 指定partition
            //kafkaProducer.send(new ProducerRecord<String,String>("first",1,null,"cpuCode" + i));

            //指定key
            //kafkaProducer.send(new ProducerRecord<String,String>("first", UUID.randomUUID().toString(),"cpuCode-->" + i));

            //黏性
            kafkaProducer.send(new ProducerRecord<String,String>("first", "cpuCode*****" + i));
        }

        //关闭对象
        kafkaProducer.close();
    }
}
