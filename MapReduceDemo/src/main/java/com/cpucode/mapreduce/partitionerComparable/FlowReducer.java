package com.cpucode.mapreduce.partitionerComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/12/1 19:41
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {

            context.write(value,key);
        }
    }
}
