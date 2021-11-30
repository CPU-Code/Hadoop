package com.cpucode.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器 需要实现Kafka提供的partitioner接口
 *
 * @author : cpucode
 * @date : 2021/11/30 16:52
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class MyPartitioner implements Partitioner {
    /**
     * 计算分区号
     * 以 first 主题为例， 有两个分区
     * 包含 cpuCode 的消息发送0号分区
     * 其他消息发送1号分区
     *
     * @param s 当前消息发往的主题
     * @param o 当前消息的key
     * @param bytes 当前消息的key序列化后字节数组
     * @param o1 当前消息的值
     * @param bytes1 当前消息的值序列化后的字节数组
     * @param cluster
     * @return
     */
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        if(o1.toString().contains("cpuCode")){
            return 0 ;
        }else{
            return 1 ;
        }
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
