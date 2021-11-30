package com.cpucode.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 该拦截器实现的功能:
 *  将时间戳添加到消息的前面
 * @author : cpucode
 * @date : 2021/11/30 19:22
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class TimeStampInterceptor implements ProducerInterceptor<String, String> {
    /**
     * 拦截器的核心处理方法
     * @param producerRecord 被拦截处理的消息
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 1. 获取消息的value
        String value = producerRecord.value();
        String result = System.currentTimeMillis() + "->" + value ;

        //2. 重新构建新的消息对象
        ProducerRecord<String, String> newRecord = new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), result);

        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
