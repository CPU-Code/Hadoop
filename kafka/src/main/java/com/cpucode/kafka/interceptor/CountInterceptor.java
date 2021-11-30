package com.cpucode.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 该拦截器实现的功能:
 *  统计发送成功和失败的消息个数
 * @author : cpucode
 * @date : 2021/11/30 19:21
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {
    private int success ;
    private int fail ;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return null;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(recordMetadata != null){
            fail ++ ;
        }else{
            success ++ ;
        }
    }

    @Override
    public void close() {
        System.out.println("SUCCESS: " + success);
        System.out.println("FAIL: " + fail);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
