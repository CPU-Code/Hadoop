package com.cpucode.kafka.partitioner;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.UUID;

/**
 * 生产者 - 自定义分区
 *
 * @author : cpucode
 * @date : 2021/11/30 16:52
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class KafkaProducerPartitioner {
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

        // 设置分区类
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.cpucode.kafka.partitioner.MyPartitioner");

        //创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // 生产数据
        for (int i = 0; i < 10; i++) {
            String value = "" ;
            if(i % 2 == 0){
                value = "cpuCode==>" + i ;
            }else{
                value = UUID.randomUUID().toString() + "==>" + i ;
            }

            kafkaProducer.send(new ProducerRecord<String, String>("first", value),
                    new Callback() {
                        /**
                         * 当消息发送完成后， 会调用该方法
                         * @param recordMetadata 消息的元数据信息.
                         * @param e 当消息发送过程中,如果抛出异常，会传入到该方法
                         */
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null){
                                System.out.println("消息发送失败: " + e.getMessage());
                            }else {
                                System.out.println("消息发送成功: " + recordMetadata.topic() +
                                        " : " + recordMetadata.partition() +
                                        " : " + recordMetadata.offset()
                                );
                            }
                        }
                    });
        }

        //关闭对象
        kafkaProducer.close();
    }
}
