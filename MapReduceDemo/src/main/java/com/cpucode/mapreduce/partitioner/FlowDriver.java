package com.cpucode.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/12/1 19:40
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        // 1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar
        job.setJarByClass(FlowDriver.class);

        // 3 关联mapper 和Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4 设置mapper 输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 指定自定义数据分区
        job.setPartitionerClass(ProvincePartitioner.class);
        // 同时指定相应数量的reduceTask
        job.setNumReduceTasks(6);

        // 6 设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\input\\partitioner"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\output\\2"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
