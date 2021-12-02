package com.cpucode.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author : cpucode
 * @date : 2021/12/2 15:17
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // text 是手机号
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        int partition;

        if ("136".equals(prePhone)){
            partition = 0;
        } else if ("137".equals(prePhone)) {
            partition = 1;
        } else if ("138".equals(prePhone)) {
            partition = 2;
        } else if ("139".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;
    }
}
